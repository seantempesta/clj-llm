(ns co.poyo.clj-llm.vertex-test
  "Unit tests for the Vertex AI backend. Network calls mocked via with-redefs
   on the HTTP layer; live integration tests are gated by VERTEX_LIVE=1 and
   live in a separate file."
  (:require
   [clojure.test :refer [deftest testing is]]
   [cheshire.core :as json]
   [co.poyo.clj-llm.backend.vertex :as vertex]
   [co.poyo.clj-llm.backend.openai :as openai]
   [co.poyo.clj-llm.content :as content]
   [co.poyo.clj-llm.protocol :as proto]
   [co.poyo.clj-llm.impl.net.jvm :as net]))

;; ════════════════════════════════════════════════════════════════════
;; URL construction
;; ════════════════════════════════════════════════════════════════════

(deftest chat-api-base-global
  (testing "Global location uses /v1beta1 and bare aiplatform hostname"
    (let [chat-api-base @#'vertex/chat-api-base
          url (chat-api-base "my-project" "global")]
      (is (= "https://aiplatform.googleapis.com/v1beta1/projects/my-project/locations/global/endpoints/openapi"
             url)))))

(deftest chat-api-base-regional
  (testing "Regional locations use /v1 and {LOCATION}- host prefix"
    (let [chat-api-base @#'vertex/chat-api-base
          url (chat-api-base "my-project" "us-central1")]
      (is (= "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/endpoints/openapi"
             url)))))

(deftest embed-url-global
  (testing "Embed URL on global location"
    (let [embed-url @#'vertex/embed-url
          url (embed-url "p" "global" "text-embedding-005")]
      (is (= "https://aiplatform.googleapis.com/v1/projects/p/locations/global/publishers/google/models/text-embedding-005:predict"
             url)))))

(deftest embed-url-regional
  (testing "Embed URL on regional location"
    (let [embed-url @#'vertex/embed-url
          url (embed-url "p" "us-east5" "gemini-embedding-001")]
      (is (= "https://us-east5-aiplatform.googleapis.com/v1/projects/p/locations/us-east5/publishers/google/models/gemini-embedding-001:predict"
             url)))))

;; ════════════════════════════════════════════════════════════════════
;; Model auto-prefix
;; ════════════════════════════════════════════════════════════════════

(deftest ensure-google-prefix-adds-when-missing
  (let [f @#'vertex/ensure-google-prefix]
    (is (= "google/gemini-3-pro-preview" (f "gemini-3-pro-preview")))))

(deftest ensure-google-prefix-idempotent
  (let [f @#'vertex/ensure-google-prefix]
    (is (= "google/gemini-3-pro-preview" (f "google/gemini-3-pro-preview")))))

(deftest ensure-google-prefix-passes-nil
  (let [f @#'vertex/ensure-google-prefix]
    (is (nil? (f nil)))))

(deftest vertex-backend-prefixes-model-in-body
  (testing "VertexBackend.build-body delegates to inner with prefixed model"
    (let [inner-calls (atom [])
          mock-inner  (reify proto/LLMProvider
                        (api-key [_] "tok")
                        (build-url [_ m] (str "https://x/" m))
                        (build-headers [_] {})
                        (build-body [_ model sp msgs s ts tc po]
                          (swap! inner-calls conj {:model model :messages msgs})
                          {:model model :messages msgs})
                        (parse-chunk [_ _ _ _] [])
                        (stream-events [_ _ _ _] nil))
          vb (vertex/->VertexBackend mock-inner "p" "global" {})]
      (proto/build-body vb "gemini-3-flash-preview" nil [{:role "user" :content "hi"}]
                        nil nil nil nil)
      (is (= "google/gemini-3-flash-preview" (:model (first @inner-calls)))
          "Inner backend received the google/-prefixed model")
      ;; Idempotent — already-prefixed model not double-prefixed.
      (reset! inner-calls [])
      (proto/build-body vb "google/gemini-3-pro-preview" nil [{:role "user" :content "hi"}]
                        nil nil nil nil)
      (is (= "google/gemini-3-pro-preview" (:model (first @inner-calls)))
          "Already-prefixed model is not double-prefixed"))))

;; ════════════════════════════════════════════════════════════════════
;; backend constructor wiring
;; ════════════════════════════════════════════════════════════════════

(deftest backend-uses-explicit-project-and-location
  (let [token-fn (constantly "fake-token-xyz")
        backend  (vertex/backend
                  {:project "my-gcp-project"
                   :location "us-central1"
                   :token-fn token-fn
                   :defaults {:model "gemini-3-pro-preview"}})]
    (is (= "my-gcp-project" (:project backend)))
    (is (= "us-central1" (:location backend)))
    (is (= "Bearer fake-token-xyz" (get (proto/build-headers backend) "Authorization"))
        "Bearer header sources token from :token-fn")))

(deftest backend-throws-without-project
  (testing "backend throws when no :project and GOOGLE_CLOUD_PROJECT env unset"
    (with-redefs [vertex/make-token-fn (constantly (constantly "tok"))]
      ;; Only run the assertion if the env var is genuinely unset, otherwise this
      ;; would false-pass on a dev machine with it set.
      (when-not (System/getenv "GOOGLE_CLOUD_PROJECT")
        (is (thrown? Exception (vertex/backend {:token-fn (constantly "x")})))))))

(deftest backend-defaults-to-global-location
  (let [b (vertex/backend {:project "p" :token-fn (constantly "t")})]
    (is (= "global" (:location b)))))

;; ════════════════════════════════════════════════════════════════════
;; Embeddings — request body construction + response parsing
;; ════════════════════════════════════════════════════════════════════

(deftest text->instance-shape
  (let [f @#'vertex/text->instance]
    (is (= {:content "hello"} (f nil nil "hello")))
    (is (= {:content "hello" :task_type "RETRIEVAL_DOCUMENT"}
           (f "RETRIEVAL_DOCUMENT" nil "hello")))
    (is (= {:content "hello" :task_type "RETRIEVAL_QUERY" :title "doc-title"}
           (f "RETRIEVAL_QUERY" "doc-title" "hello")))))

(deftest parse-embed-response-shape
  (let [f @#'vertex/parse-embed-response
        canned (json/generate-string
                {:predictions [{:embeddings {:values [0.1 0.2 0.3]
                                             :statistics {:token_count 5 :truncated false}}}
                               {:embeddings {:values [0.4 0.5 0.6]
                                             :statistics {:token_count 3 :truncated false}}}]})
        result (f canned)]
    (is (= [[0.1 0.2 0.3] [0.4 0.5 0.6]] (:embeddings result)))
    (is (= 2 (count (:stats result))))
    (is (= 8 (get-in result [:usage :total-tokens])))))

(deftest embed-builds-correct-request
  (testing "embed POSTs the right URL with the right body shape"
    (let [captured (atom nil)
          fake-resp (json/generate-string
                     {:predictions [{:embeddings {:values [0.1 0.2]
                                                  :statistics {:token_count 1}}}]})]
      (with-redefs [net/post-stream
                    (fn [url headers body]
                      (reset! captured {:url url :headers headers :body body})
                      {:status 200
                       :body   (java.io.ByteArrayInputStream. (.getBytes ^String fake-resp "UTF-8"))})]
        (let [backend (vertex/backend {:project "test-project"
                                       :token-fn (constantly "fake-token")})
              result  (vertex/embed backend
                                    {:model "text-embedding-005"
                                     :inputs ["hello"]
                                     :task-type "RETRIEVAL_DOCUMENT"
                                     :dimensions 256})]
          (is (= [[0.1 0.2]] (:embeddings result)))
          (let [{:keys [url headers body]} @captured
                parsed-body (json/parse-string body true)]
            (is (= "https://aiplatform.googleapis.com/v1/projects/test-project/locations/global/publishers/google/models/text-embedding-005:predict"
                   url))
            (is (= "Bearer fake-token" (get headers "Authorization")))
            (is (= [{:content "hello" :task_type "RETRIEVAL_DOCUMENT"}]
                   (:instances parsed-body)))
            (is (= {:outputDimensionality 256} (:parameters parsed-body)))))))))

(deftest embed-rejects-non-string-model
  (let [backend (vertex/backend {:project "p" :token-fn (constantly "t")})]
    (is (thrown? Exception (vertex/embed backend {:model nil :inputs ["x"]})))
    (is (thrown? Exception (vertex/embed backend {:model 42 :inputs ["x"]})))))

(deftest embed-rejects-empty-inputs
  (let [backend (vertex/backend {:project "p" :token-fn (constantly "t")})]
    (is (thrown? Exception (vertex/embed backend {:model "m" :inputs []})))
    (is (thrown? Exception (vertex/embed backend {:model "m" :inputs nil})))))

(deftest embed-surfaces-non-200-with-retryable-flag
  (let [error-body "{\"error\": {\"code\": 429, \"message\": \"quota\"}}"]
    (with-redefs [net/post-stream
                  (fn [_ _ _]
                    {:status 429
                     :body (java.io.ByteArrayInputStream. (.getBytes ^String error-body "UTF-8"))})]
      (let [backend (vertex/backend {:project "p" :token-fn (constantly "t")})
            thrown (try (vertex/embed backend {:model "m" :inputs ["x"]})
                        (catch Exception e e))]
        (is (instance? Exception thrown))
        (is (= 429 (:status (ex-data thrown))))
        (is (true? (:retryable? (ex-data thrown))))))))

;; ════════════════════════════════════════════════════════════════════
;; Vision — gs:// content part
;; ════════════════════════════════════════════════════════════════════

(deftest content-image-passes-gs-uri-as-url-source
  (testing "content/image emits :source :url for gs:// URIs (no download/encode)"
    (let [part (content/image "gs://my-bucket/photo.jpg")]
      (is (= :image (:type part)))
      (is (= :url (:source part)))
      (is (= "gs://my-bucket/photo.jpg" (:url part)))
      ;; Critically, NO :data or :media-type — meaning we did not try to
      ;; download/base64 the GCS object.
      (is (not (contains? part :data)))
      (is (not (contains? part :media-type))))))

(deftest content-image-still-handles-https
  (testing "Regression: https URLs still work (without resize → pass-through)"
    (let [part (content/image "https://example.com/img.png")]
      (is (= :url (:source part)))
      (is (= "https://example.com/img.png" (:url part))))))

(deftest openai-emits-image-url-for-gs-source
  (testing "openai backend serializes :source :url (gs://) as standard image_url part"
    (let [c->o @#'co.poyo.clj-llm.backend.openai/content-part->openai
          part (c->o {:type :image :source :url :url "gs://bucket/foo.jpg"})]
      (is (= {:type "image_url"
              :image_url {:url "gs://bucket/foo.jpg"}}
             part)))))
