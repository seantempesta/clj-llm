(ns co.poyo.clj-llm.backend.vertex
  "Google Cloud Vertex AI backend for Gemini models. JVM-only.

   Chat + vision go through Vertex's OpenAI-compatible endpoint:
     https://aiplatform.googleapis.com/v1beta1/projects/{P}/locations/global/endpoints/openapi/chat/completions
   We reuse co.poyo.clj-llm.backend.openai for the chat-completions path
   and wrap it with model-name auto-prefix (`google/`) and an ADC-refreshed
   bearer token. The Vertex OpenAI-compat surface accepts the standard
   `image_url` content part — base64 data URLs, https URLs, and gs:// GCS
   URIs all work.

   Embeddings are NOT exposed on the OpenAI-compat surface; they go through
   the native :predict endpoint. See `embed`.

   Auth: Application Default Credentials (ADC). Run
     gcloud auth application-default login
   on the dev machine, or use a service account in production. Tokens are
   cached and refreshed in-process.

   Usage:
     (require '[co.poyo.clj-llm.backend.vertex :as vertex])
     (def gemini
       (vertex/backend {:project  \"my-gcp-project\"
                        :defaults {:model \"gemini-3-pro-preview\"}}))
     (llm/generate gemini \"hello\")"
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]
   [co.poyo.clj-llm.backend.openai :as openai]
   [co.poyo.clj-llm.impl.net.jvm :as net]
   [co.poyo.clj-llm.protocol :as proto])
  (:import
   [com.google.auth.oauth2 GoogleCredentials]))

;; ════════════════════════════════════════════════════════════════════
;; ADC token refresh
;; ════════════════════════════════════════════════════════════════════

(def ^:private cloud-platform-scope
  "https://www.googleapis.com/auth/cloud-platform")

(def ^:private refresh-margin-ms
  "Refresh the ADC access token this many ms before its stated expiry.
   Avoids races where multiple JVM threads each see an about-to-expire token
   and trigger redundant refreshes."
  60000)

(defn make-token-fn
  "Build a zero-arg fn that returns a fresh GCP access token sourced from
   Application Default Credentials. Token is cached in an atom and refreshed
   when within `refresh-margin-ms` of expiry.

   The returned fn is safe to pass as `:api-key` to co.poyo.clj-llm.backend.openai
   — that backend invokes the api-key fn on every request via build-headers,
   so token refresh is transparent."
  []
  (let [creds (-> (GoogleCredentials/getApplicationDefault)
                  (.createScoped ^java.util.List [cloud-platform-scope]))
        cache (atom nil)]
    (fn []
      (let [token @cache
            now   (System/currentTimeMillis)
            valid? (and token
                        (when-let [exp (.getExpirationTime token)]
                          (> (- (.getTime exp) now) refresh-margin-ms)))]
        (if valid?
          (.getTokenValue token)
          (do (.refreshIfExpired creds)
              (let [new-token (.getAccessToken creds)]
                (reset! cache new-token)
                (.getTokenValue new-token))))))))

;; ════════════════════════════════════════════════════════════════════
;; URLs
;; ════════════════════════════════════════════════════════════════════

(defn- chat-api-base
  "Vertex's OpenAI-compat endpoints/openapi path.
   Global location uses /v1beta1 and the bare aiplatform.googleapis.com host;
   regional locations use /v1 with a {LOCATION}- host prefix."
  [project location]
  (if (= "global" location)
    (str "https://aiplatform.googleapis.com/v1beta1/projects/" project
         "/locations/global/endpoints/openapi")
    (str "https://" location "-aiplatform.googleapis.com/v1/projects/" project
         "/locations/" location "/endpoints/openapi")))

(defn- embed-url
  "Native :predict endpoint URL for an embedding model. Used by `embed`."
  [project location model]
  (if (= "global" location)
    (str "https://aiplatform.googleapis.com/v1/projects/" project
         "/locations/global/publishers/google/models/" model ":predict")
    (str "https://" location "-aiplatform.googleapis.com/v1/projects/" project
         "/locations/" location "/publishers/google/models/" model ":predict")))

;; ════════════════════════════════════════════════════════════════════
;; Backend record — delegates to inner OpenAIBackend with model auto-prefix
;; ════════════════════════════════════════════════════════════════════

(defn- ensure-google-prefix
  "Vertex requires Gemini model names to carry a `google/` publisher prefix.
   Idempotent — won't double-prefix."
  [model]
  (cond
    (nil? model) nil
    (str/starts-with? model "google/") model
    :else (str "google/" model)))

(defrecord VertexBackend [inner project location defaults]
  proto/LLMProvider
  (api-key [_] (proto/api-key inner))
  (build-url [_ model] (proto/build-url inner model))
  (build-headers [_] (proto/build-headers inner))
  (build-body [_ model system-prompt messages schema tools tool-choice provider-opts]
    (proto/build-body inner (ensure-google-prefix model)
                      system-prompt messages schema tools tool-choice provider-opts))
  (parse-chunk [_ chunk schema tools]
    (proto/parse-chunk inner chunk schema tools))
  (stream-events [_ url headers body]
    (proto/stream-events inner url headers body)))

(defn backend
  "Create a Vertex AI backend.

   Options:
     :project  - GCP project ID (string). Defaults to GOOGLE_CLOUD_PROJECT env.
     :location - GCP location string, default \"global\". Note: Gemini 3.x
                 preview models only resolve on the global endpoint as of 2026-05.
     :defaults - default opts merged into every call (typically {:model \"...\"}).
     :token-fn - optional zero-arg fn returning an access token string. Override
                 for tests; production should use the default (ADC via make-token-fn).

   Models passed via :model use bare Gemini names (e.g. \"gemini-3-pro-preview\").
   The `google/` publisher prefix is added automatically before sending to Vertex.

   Example:
     (def gemini
       (vertex/backend
         {:project  \"your-gcp-project-id\"
          :defaults {:model \"gemini-3-pro-preview\"}}))
     (llm/generate gemini \"hello\")
     (llm/generate gemini
                   [(content/text \"What's in this image?\")
                    (content/image \"gs://bucket/photo.jpg\")])"
  ([] (backend {}))
  ([{:keys [project location defaults token-fn]
     :or   {location "global"}}]
   (let [project (or project
                     (System/getenv "GOOGLE_CLOUD_PROJECT")
                     (throw (ex-info "Vertex backend requires :project option or GOOGLE_CLOUD_PROJECT env var"
                                     {:error-type :llm/invalid-request})))
         token-fn (or token-fn (make-token-fn))
         inner (openai/backend {:api-key  token-fn
                                :api-base (chat-api-base project location)
                                :defaults defaults})]
     (->VertexBackend inner project location defaults))))

;; ════════════════════════════════════════════════════════════════════
;; Embeddings — native :predict (not on the OpenAI-compat surface)
;; ════════════════════════════════════════════════════════════════════

(defn- text->instance
  "Build a single Vertex embed instance map from text + optional task type / title."
  [task-type title text]
  (cond-> {:content text}
    task-type (assoc :task_type task-type)
    title     (assoc :title title)))

(defn- parse-embed-response
  "Convert a Vertex :predict response body string into our embed result map."
  [response-body]
  (let [parsed       (json/parse-string response-body true)
        predictions  (:predictions parsed)
        embeddings   (mapv #(get-in % [:embeddings :values]) predictions)
        stats        (mapv #(get-in % [:embeddings :statistics]) predictions)
        total-tokens (reduce + 0 (keep :token_count stats))]
    (cond-> {:embeddings embeddings}
      (some seq stats)     (assoc :stats stats)
      (pos? total-tokens)  (assoc :usage {:total-tokens total-tokens}))))

(def ^:private retryable-statuses #{408 429 500 502 503 504})

(defn embed
  "Compute text embeddings via Vertex AI's native :predict endpoint.

   (vertex/embed gemini
                 {:model \"text-embedding-005\"
                  :inputs [\"first document\" \"second document\"]
                  :task-type \"RETRIEVAL_DOCUMENT\"
                  :dimensions 768})
   ;; => {:embeddings [[0.1 0.2 …] [0.3 0.4 …]]
   ;;     :stats [{:token_count 5 :truncated false} …]
   ;;     :usage {:total-tokens 11}}

   Options:
     :model      embedding model name (NO `google/` prefix — embeddings live
                 under `publishers/google/models/` in the URL, so the model
                 path component is bare). Common choices mid-2026:
                   \"text-embedding-005\"             — English, 768 dim
                   \"text-multilingual-embedding-002\" — multilingual, 768 dim
                   \"gemini-embedding-001\"           — Matryoshka, 768/1536/3072
     :inputs     vector of strings to embed (max 250 per request, 8K tokens each,
                 aggregate cap 20K tokens per request).
     :task-type  one of RETRIEVAL_DOCUMENT, RETRIEVAL_QUERY, SEMANTIC_SIMILARITY,
                 CLASSIFICATION, CLUSTERING, CODE_RETRIEVAL_QUERY,
                 QUESTION_ANSWERING, FACT_VERIFICATION. Optional but recommended
                 for retrieval — use _DOCUMENT for indexing, _QUERY for searches.
     :dimensions Matryoshka truncation (gemini-embedding-001 only).
     :title      optional title applied to every instance.
     :auto-truncate boolean — if true, long inputs are truncated server-side
                 rather than rejected.

   Throws ex-info on non-200 responses. No automatic retry — callers wanting
   robustness against 429/5xx should wrap in their own retry loop."
  [{:keys [project location] :as backend}
   {:keys [model inputs task-type dimensions title auto-truncate]}]
  (when-not (string? model)
    (throw (ex-info "embed requires :model (string)"
                    {:error-type :llm/invalid-request :model model})))
  (when-not (and (sequential? inputs) (seq inputs))
    (throw (ex-info "embed requires non-empty :inputs vector"
                    {:error-type :llm/invalid-request :inputs inputs})))
  (let [url        (embed-url project location model)
        headers    (proto/build-headers backend)
        instances  (mapv (partial text->instance task-type title) inputs)
        params     (cond-> {}
                     dimensions            (assoc :outputDimensionality dimensions)
                     (some? auto-truncate) (assoc :autoTruncate auto-truncate))
        request-body (json/generate-string
                      (cond-> {:instances instances}
                        (seq params) (assoc :parameters params)))
        {:keys [status body]} (net/post-stream url headers request-body)
        body-str (try (slurp body) (catch Exception _ nil))]
    (when (not= 200 status)
      (throw (ex-info (str "Vertex embed HTTP " status
                           (when body-str (str ": " body-str)))
                      {:error-type :llm/server-error
                       :status     status
                       :body       body-str
                       :retryable? (boolean (retryable-statuses status))})))
    (parse-embed-response body-str)))
