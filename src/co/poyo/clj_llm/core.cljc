(ns co.poyo.clj-llm.core
  (:require
   [clojure.core.async :as a]
   [clojure.set]
   [clojure.string :as str]
   [malli.core :as m]
   [malli.error :as me]
   [malli.transform :as mt]
   [malli.util :as mu]
   [co.poyo.clj-llm.protocol :as proto]
   [co.poyo.clj-llm.content :as content]
   [co.poyo.clj-llm.stream :as stream]
   #?(:clj [cheshire.core :as json])))

;; ════════════════════════════════════════════════════════════════════
;; Cross-platform shims
;; ════════════════════════════════════════════════════════════════════

(defn- now-ms []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn- json-stringify [v]
  #?(:clj  (json/generate-string v)
     :cljs (js/JSON.stringify (clj->js v))))

(defn- json-parse-keywordize [s]
  #?(:clj  (json/parse-string s true)
     :cljs (js->clj (js/JSON.parse s) :keywordize-keys true)))

(defn- err-msg [e]
  #?(:clj  (.getMessage ^Throwable e)
     :cljs (or (.-message e) (str e))))

(defn- throwable? [v]
  #?(:clj  (instance? Throwable v)
     :cljs (instance? js/Error v)))

;; ════════════════════════════════════════════════════════════════════
;; Option schemas
;; ════════════════════════════════════════════════════════════════════

(def ^:private opts-schema
  "Schema for generate/request options."
  [:map {:closed true}
   [:model {:optional true} :string]
   [:system-prompt {:optional true} :string]
   [:schema {:optional true} :any]
   [:tools {:optional true} :any]
   [:tool-choice {:optional true} :any]
   [:temperature {:optional true} number?]
   [:max-tokens {:optional true} :int]
   [:top-p {:optional true} number?]
   ;; DeepSeek thinking-mode controls (also accepted by some other reasoning models
   ;; via the OpenAI-compat surface). Passed through to the provider unchanged.
   ;; :thinking — map like {:type "enabled"} or {:type "disabled"}
   ;; :reasoning-effort — "high" | "max" (DeepSeek), "low" | "medium" | "high" (OpenAI)
   [:thinking {:optional true} :any]
   [:reasoning-effort {:optional true} :string]
   ;; Raw provider response_format. Use this for JSON mode ({:type "json_object"})
   ;; or provider-specific structured-output (Vertex's json_schema). For Malli-driven
   ;; structured output prefer :schema, which uses the tool-call mechanism.
   [:response-format {:optional true} :any]
   ;; Retry config for transient HTTP failures (429, transient 5xx) and provider
   ;; mid-stream pressure signals (e.g. DeepSeek's `insufficient_system_resource`
   ;; finish_reason). Retry only fires before any event has been forwarded to
   ;; the consumer — once a chunk has been emitted the stream is committed.
   ;; Pass {:max-attempts 0} (or {:max-attempts 1}) to disable.
   [:retry {:optional true}
    [:map
     [:max-attempts {:optional true} :int]
     [:base-delay-ms {:optional true} :int]
     [:max-delay-ms {:optional true} :int]]]
   [:provider-opts {:optional true} [:map-of :keyword :any]]
   [:on-text {:optional true} fn?]
   [:on-reasoning {:optional true} fn?]
   [:on-tool-calls {:optional true} fn?]
   [:on-tool-result {:optional true} fn?]])

(def ^:private agent-opts-schema
  "Schema for run-agent options (superset of opts-schema)."
  (mu/merge opts-schema
            [:map {:closed true}
             [:max-steps {:optional true} :int]
             [:stop-when {:optional true} fn?]]))

;; Keys that get forwarded to the provider API (not consumed by clj-llm itself).
;; All of these survive snake_case conversion at the backend boundary (see
;; backend/openai.cljc convert-options-for-api), so kebab-case keys here become
;; snake_case JSON fields automatically.
(def ^:private api-forward-keys
  #{:temperature :max-tokens :top-p
    :thinking :reasoning-effort :response-format})

(defn- parse-opts
  "Parse options against a malli schema. Returns a map with clj-llm keys
   and :provider-opts merged from api-forward-keys + explicit :provider-opts.
   Throws on unknown keys or invalid types."
  ([opts] (parse-opts opts opts-schema))
  ([opts schema]
   (let [explanation (m/explain schema opts)]
     (when explanation
       (throw (ex-info
               (str "Invalid options: " (pr-str (me/humanize explanation)))
               {:error-type :llm/invalid-request
                :errors     (me/humanize explanation)})))
     (let [provider-opts (not-empty
                          (merge
                           (-> (select-keys opts api-forward-keys)
                               (clojure.set/rename-keys {:max-tokens :max_tokens}))
                           (:provider-opts opts)))]
       (cond-> (apply dissoc opts (concat api-forward-keys [:provider-opts]))
         provider-opts (assoc :provider-opts provider-opts))))))

;; ════════════════════════════════════════════════════════════════════
;; Event state machine
;; ════════════════════════════════════════════════════════════════════

(def ^:private init-state
  "Initial accumulator for the event stream consumer."
  {:chunks [] :reasoning-chunks [] :tool-calls [] :tool-call-positions {} :usage {} :finish-reason nil :error nil})

(defn- next-state
  "Pure state transition: state × event → state'.
   No side effects."
  [state event]
  (case (:type event)
    :content
    (update state :chunks conj (:content event))

    :reasoning
    (update state :reasoning-chunks conj (:content event))

    :tool-call
    (let [idx  (or (:index event) (count (:tool-calls state)))
          call (assoc event :arguments (or (:arguments event) ""))]
      (-> state
          (update :tool-calls conj call)
          (assoc-in [:tool-call-positions idx] (count (:tool-calls state)))))

    :tool-call-delta
    (let [pos (get (:tool-call-positions state) (:index event))]
      (if pos
        (update-in state [:tool-calls pos :arguments] str (:arguments event))
        state))

    :usage
    (update state :usage merge (dissoc event :type))

    :finish
    (assoc state :finish-reason (:reason event))

    :error
    (let [err (:error event)
          msg (or (:message err) (pr-str err))]
      (assoc state :error (ex-info (str "LLM error: " msg)
                                   {:error-type :llm/server-error :error err})))

    :done state

    ;; Forward-compat: ignore event types we don't recognize yet
    state))

(defn- finalize-state
  "Turn accumulated state into a result map."
  [{:keys [chunks reasoning-chunks error usage finish-reason tool-calls]}]
  (if error
    (throw error)
    (cond-> {:text (apply str chunks)}
      (seq reasoning-chunks) (assoc :reasoning (apply str reasoning-chunks))
      (seq usage)      (assoc :usage (cond-> usage finish-reason (assoc :finish-reason finish-reason)))
      (seq tool-calls) (assoc :tool-calls tool-calls))))

;; ════════════════════════════════════════════════════════════════════
;; Internal helpers
;; ════════════════════════════════════════════════════════════════════

(defn- parse-structured-output
  "Parse JSON text and validate against a malli schema."
  [text schema]
  (try
    (let [parsed (json-parse-keywordize text)
          result (m/decode schema parsed mt/json-transformer)]
      (if (m/validate schema result)
        result
        (throw (ex-info
                "Schema validation failed"
                {:error-type :llm/invalid-request
                 :schema schema
                 :value result
                 :errors (me/humanize (m/explain schema result))}))))
    ;; Re-throw our own errors (schema validation); only catch parse failures below
    (catch #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo) e (throw e))
    (catch #?(:clj Exception :cljs :default) _
      (throw (ex-info
              "Failed to parse structured output"
              {:error-type :llm/invalid-request
               :schema schema :input text})))))

(defn- mixed-content-vector?
  "Returns true if v is a vector containing content parts (images, text parts)
   mixed with strings — i.e. a multimodal user message, not a message history."
  [v]
  (and (vector? v)
       (some content/content-part? v)))

(defn- normalize-content-element
  "Coerce an element of a mixed content vector to a content part.
   Strings become text parts. Content parts pass through."
  [x]
  (cond
    (string? x) (content/text x)
    (content/content-part? x) x
    :else (throw (ex-info
                  (str "Invalid content element: expected string or content part, got " (type x))
                  {:error-type :llm/invalid-request :element x}))))

(defn- build-messages
  "Coerce input to a messages vector.
   Map     → auto-unwrap :text (result from previous generate/run-agent)
   String  → [{:role :user :content input}]
   Vector of content parts/strings → [{:role :user :content [...parts...]}]
   Vector of messages → used as-is (message history)
   nil     → []"
  [input]
  (cond
    (:structured input) [{:role :user :content (prn-str (:structured input))}]
    (:text input)        [{:role :user :content (:text input)}]
    (string? input) [{:role :user :content input}]
    (mixed-content-vector? input)
    [{:role :user :content (mapv normalize-content-element input)}]
    (vector? input) input
    (nil? input)    []
    :else (throw (ex-info
                  (str "Input must be a string, vector, or nil — got " (type input))
                  {:error-type :llm/invalid-request
                   :input input}))))

(defn- provider-request-events-once
  "Orchestrate one attempt at a provider request using LLMProvider protocol
   methods. Returns a core.async channel of events for this single attempt.
   No retry — see `events-with-retry` for the wrapper that retries on
   transient errors."
  [provider request]
  (let [{:keys [model system-prompt messages schema tools tool-choice provider-opts]} request
        body-map (proto/build-body provider model system-prompt messages
                                   schema tools tool-choice provider-opts)
        url (proto/build-url provider model)
        headers (proto/build-headers provider)
        body (json-stringify body-map)]
    (let [raw-ch (proto/stream-events provider url headers body)
          out-ch (a/chan 256)]
      (a/go
        (loop []
          (let [v (a/<! raw-ch)]
            (cond
              (nil? v)                (a/close! out-ch)
              (throwable? v)          (do (a/>! out-ch v) (a/close! out-ch))
              :else
              (do
                (doseq [event (proto/parse-chunk provider v schema tools)]
                  (a/>! out-ch event))
                (recur))))))
      out-ch)))

(def ^:private default-retry
  {:max-attempts 3 :base-delay-ms 500 :max-delay-ms 30000})

(defn- retryable-event?
  "True if v is a Throwable or an :error event flagged retryable.
   Retryability is set by the HTTP layer (429 + transient 5xx) and by the
   provider parse-chunk (e.g. DeepSeek's `insufficient_system_resource`
   finish_reason)."
  [v]
  (cond
    (throwable? v) (true? (:retryable? (ex-data v)))
    (and (map? v) (= :error (:type v)))
    (true? (get-in v [:error :retryable?]))
    :else false))

(defn- compute-backoff
  "Exponential backoff with a hard ceiling. attempt is 1-indexed."
  [base-delay-ms max-delay-ms attempt]
  (let [exp #?(:clj  (Math/pow 2 (dec attempt))
               :cljs (js/Math.pow 2 (dec attempt)))]
    (long (min (double max-delay-ms) (* (double base-delay-ms) exp)))))

(defn- provider-request-events
  "Run a provider request with retry on transient errors. Retries only as long
   as no event has been forwarded to the consumer — once a chunk has been
   passed through, the stream is considered committed and further errors flow
   through normally.

   Retryable signals: HTTP 408 / 429 / 5xx (set by net+stream layer), provider
   `:error` events with :retryable? true (e.g. DeepSeek insufficient_system_resource)."
  [provider request]
  (let [retry (merge default-retry (:retry request))
        {:keys [max-attempts base-delay-ms max-delay-ms]} retry
        out-ch (a/chan 256)]
    (a/go
      (loop [attempt 1]
        (let [stream-ch     (provider-request-events-once provider request)
              committed?    (atom false)
              should-retry? (atom false)]
          (loop []
            (let [v (a/<! stream-ch)]
              (cond
                (nil? v) nil

                (and (retryable-event? v)
                     (not @committed?)
                     (< attempt max-attempts))
                (reset! should-retry? true)

                :else
                (do (reset! committed? true)
                    (a/>! out-ch v)
                    (recur)))))
          (if @should-retry?
            (do (a/<! (a/timeout (compute-backoff base-delay-ms max-delay-ms attempt)))
                (recur (inc attempt)))
            (a/close! out-ch)))))
    out-ch))

;; ════════════════════════════════════════════════════════════════════
;; Core API
;; ════════════════════════════════════════════════════════════════════

(def ^:dynamic *stream-timeout-ms*
  "Maximum milliseconds to wait for a single event from the provider stream.
   Default 5 minutes. Bind to a smaller value for interactive use."
  (* 5 60 1000))

#?(:clj
   (defn- chan-reduce
     "Blocking reduce over a core.async channel.
      Returns the final accumulated value.  Closes ch on early termination.
      Throws on timeout (configurable via *stream-timeout-ms*)."
     [rf init ch]
     (loop [acc init]
       (let [timeout-ch (a/timeout *stream-timeout-ms*)
             [v port] (a/alts!! [ch timeout-ch])]
         (cond
           (= port timeout-ch)
           (do (a/close! ch)
               (throw (ex-info (str "LLM stream timed out after " *stream-timeout-ms* "ms")
                               {:error-type :llm/timeout})))
           (nil? v) acc
           (instance? Throwable v) (throw v)
           :else (let [acc' (rf acc v)]
                   (if (reduced? acc')
                     (do (a/close! ch) @acc')
                     (recur acc'))))))))

(defn chan-reduce-ch
  "Cross-platform reduce over a core.async channel.

   Returns a core.async channel that will yield exactly one value: either
   the final reduced accumulator, or a Throwable/ex-info if the stream
   errors or times out.

   Used directly by the CLJS `generate-promise` path and by JVM callers
   who want non-blocking semantics."
  [rf init ch]
  (let [out (a/chan 1)
        timeout-ms *stream-timeout-ms*]
    (a/go
      (loop [acc init]
        (let [timeout-ch (a/timeout timeout-ms)
              [v port] (a/alts! [ch timeout-ch])]
          (cond
            (= port timeout-ch)
            (do (a/close! ch)
                (a/>! out (ex-info (str "LLM stream timed out after " timeout-ms "ms")
                                   {:error-type :llm/timeout}))
                (a/close! out))

            (nil? v)
            (do (a/>! out acc) (a/close! out))

            (throwable? v)
            (do (a/>! out v) (a/close! out))

            :else
            (let [acc' (try (rf acc v) (catch #?(:clj Exception :cljs :default) e e))]
              (cond
                (throwable? acc')
                (do (a/>! out acc') (a/close! out))

                (reduced? acc')
                (do (a/close! ch) (a/>! out @acc') (a/close! out))

                :else
                (recur acc')))))))
    out))

(defn events
  "Return a bounded core.async channel of provider events.
   Close the channel to signal cancellation. Note: the underlying HTTP body stream
   will drain until the server closes the connection — for immediate cancellation,
   set a short *stream-timeout-ms* binding before calling.

   Events are maps with :type — :content, :tool-call, :tool-call-delta,
   :usage, :finish, :error, :done."
  ([provider input] (events provider {} input))
  ([provider opts input]
   (let [{:keys [model system-prompt schema tools tool-choice provider-opts retry] :as parsed}
         (parse-opts (merge (:defaults provider) opts))
         _ (when-not model
             (throw (ex-info "No model specified"
                             {:error-type :llm/invalid-request :opts parsed})))]
     (provider-request-events provider
       {:model model :system-prompt system-prompt
        :messages (build-messages input)
        :schema schema :tools tools :tool-choice tool-choice
        :provider-opts (or provider-opts {})
        :retry retry}))))

(defn- parse-tool-calls
  "Parse JSON argument strings in tool calls.
   Throws on malformed JSON so callers get a clear error."
  [raw-tool-calls]
  (when raw-tool-calls
    (mapv (fn [t]
            (let [raw (:arguments t)]
              {:id   (:id t)
               :name (:name t)
               :arguments
               (if (string? raw)
                 (try (json-parse-keywordize raw)
                      (catch #?(:clj Exception :cljs :default) e
                        (throw (ex-info (str "Malformed tool call arguments for " (:name t) ": " (err-msg e))
                                        {:error-type :llm/server-error
                                         :tool       (:name t)
                                         :arguments  raw}))))
                 raw)}))
          raw-tool-calls)))

(defn- tool-calls->assistant-message
  "Build the assistant message for tool call history round-tripping.
   Includes :content when the model returned text alongside tool calls.
   Includes :reasoning-content when the model emitted reasoning — required
   by DeepSeek's thinking-mode tool-call multi-turn contract; backends that
   don't recognize the field will simply pass it through harmlessly."
  [tool-calls text reasoning]
  (cond-> {:role :assistant
           :tool-calls (mapv (fn [{:keys [id name arguments]}]
                               {:id id :type "function"
                                :function {:name name
                                           :arguments (if (string? arguments)
                                                        arguments
                                                        (json-stringify arguments))}})
                             tool-calls)}
    (seq text)      (assoc :content text)
    (seq reasoning) (assoc :reasoning-content reasoning)))

(defn- resolve-tool-schema
  "Get the Malli function schema ([:=> ...] or [:-> ...]) from a tool.
   Checks in order:
   1. :malli/schema on metadata (defn + {:malli/schema ...}, with-meta)
   2. :schema on metadata (mx/defn)
   3. Malli global function registry (m/=> annotation)"
  [tool]
  (let [m (meta tool)]
    (or (:malli/schema m)
        (:schema m)
        ;; Check Malli's global function registry (populated by m/=>)
        (when-let [ns-sym (some-> (:ns m) ns-name)]
          (:schema (get-in (m/function-schemas) [ns-sym (:name m)])))
        (throw (ex-info
                "Tool missing Malli schema. Use {:malli/schema ...} and pass function as a #'var, mx/defn, or m/=>."
                {:error-type :llm/invalid-request :tool tool})))))

(defn- extract-input-schema
  "Extract the input map schema from a Malli function schema.
   Supports [:=> [:cat <map-schema>] <ret>] and [:-> <map-schema> <ret>]."
  [fn-schema]
  (let [fn-schema (m/schema fn-schema)
        schema-type (m/type fn-schema)
        ;; :-> is a flat proxy for :=> -- deref to normalize
        resolved (if (= :-> schema-type) (m/deref fn-schema) fn-schema)
        resolved-type (m/type resolved)]
    (when-not (= :=> resolved-type)
      (throw (ex-info
              "Tool schema must be [:=> [:cat ...] <return>] or [:-> <input> <return>]"
              {:error-type :llm/invalid-request :schema fn-schema})))
    (let [input-schema (first (m/children resolved))
          args (m/children (m/schema input-schema))]
      (when (empty? args)
        (throw (ex-info
                "Tool function schema has no input arguments"
                {:error-type :llm/invalid-request :schema fn-schema})))
       (first args))))

(defn- extract-tool-name
  "Get the tool name from a Malli schema's properties."
  [schema]
  (let [props (try (m/properties (m/schema schema))
                   (catch #?(:clj Exception :cljs :default) _ nil))]
    (or (:name props)
        (throw (ex-info
                "Tool schema missing :name in properties"
                {:error-type :llm/invalid-request :schema schema})))))

(defn- tools->input-schemas
  "Extract Malli input schemas from a vector of tool vars/fns."
  [tools]
  (mapv (fn [tool]
          (let [tool-name (last (str/split (str tool) #"/"))
                tool-schema (resolve-tool-schema tool)
                input-schema (extract-input-schema tool-schema)]
            (cond-> input-schema
                (not (:name (m/properties input-schema)))
                (mu/update-properties assoc :name tool-name))))
        tools))

(defn- build-name->fn
  "Build a map from tool name (string) to tool function."
  [tools input-schemas]
  (into {} (map (fn [t s] [(extract-tool-name s) t]) tools input-schemas)))

(defn- execute-tool-call
  "Look up and execute a single tool call. Returns the result."
  [name->fn tool-call]
  (let [f (or (get name->fn (:name tool-call))
              (throw (ex-info
                      (str "Unknown tool: " (:name tool-call))
                      {:error-type :llm/invalid-request
                       :name       (:name tool-call)
                       :available  (keys name->fn)})))]
    (f (:arguments tool-call))))

(defn- serialize-tool-result
  "Serialize a tool result to a string for the LLM.
   Strings pass through; anything else becomes JSON."
  [v]
  (if (string? v)
    v
    (json-stringify v)))

(defn tool-result
  "Create a tool result message for feeding back into message history.

   (tool-result \"call_abc\" \"Sunny, 22°C\")
   ;; => {:role :tool :tool-call-id \"call_abc\" :content \"Sunny, 22°C\"}

   Non-string values are automatically JSON-encoded:

   (tool-result \"call_abc\" {:name \"Tokyo\" :latitude 35.69})
   ;; => {:role :tool :tool-call-id \"call_abc\" :content \"{\\\"name\\\":\\\"Tokyo\\\",...}\"}"
  [tool-call-id content]
  {:role :tool :tool-call-id tool-call-id :content (serialize-tool-result content)})

;; ════════════════════════════════════════════════════════════════════
;; generate — JVM (blocking) and CLJS (Promise)
;; ════════════════════════════════════════════════════════════════════

(defn- finalize-with-tools-and-schema
  "Post-process a finalized state map: execute tool calls, parse structured
   output, attach base metadata. Pure-ish (calls on-tool-calls / on-tool-result
   callbacks if provided)."
  [{:keys [tools schema input-schemas name->fn on-tool-calls on-tool-result base]} result]
  (cond
    tools
    (let [parsed-calls (or (parse-tool-calls (:tool-calls result)) [])
          _            (when (and on-tool-calls (seq parsed-calls))
                         (on-tool-calls {:tool-calls parsed-calls
                                         :text (not-empty (:text result))}))
          tool-results (mapv (fn [tc]
                               (let [res (try
                                           {:result (execute-tool-call name->fn tc)}
                                           (catch #?(:clj Exception :cljs :default) e
                                             {:result (str "Error: " (err-msg e))
                                              :error e}))]
                                 (when on-tool-result
                                   (on-tool-result {:tool-call tc
                                                    :result (:result res)
                                                    :error (:error res)}))
                                 (:result res)))
                             parsed-calls)]
      (merge base {:tool-calls   parsed-calls
                   :tool-results tool-results}))

    schema
    (merge base {:structured (parse-structured-output (:text result) schema)})

    :else base))

(defn- generate-reducer
  "Build the reducer closure used by both blocking and async paths."
  [{:keys [on-text on-reasoning timings]}]
  (fn [state event]
    (when (= :reasoning (:type event))
      (swap! timings update :reasoning-start #(or % (now-ms)))
      (when on-reasoning (on-reasoning (:content event))))
    (when (= :content (:type event))
      (swap! timings update :content-start #(or % (now-ms)))
      (when on-text (on-text (:content event))))
    (next-state state event)))

(defn- build-base
  "Assemble the base result map (timings + top-level text/reasoning/usage)."
  [{:keys [start-time end-time timings model result]}]
  (let [{:keys [reasoning-start content-start]} @timings]
    (cond-> {:timings {:duration-ms (- end-time start-time)}}
      reasoning-start (update :timings assoc
                              :reasoning {:start-ms (- reasoning-start start-time)
                                          :duration-ms (- (or content-start end-time) reasoning-start)})
      content-start (update :timings assoc
                            :text {:start-ms (- content-start start-time)
                                   :duration-ms (- end-time content-start)})
      (not-empty (:text result))      (assoc :text (:text result))
      (not-empty (:reasoning result)) (assoc :reasoning (:reasoning result))
      (:usage result)                 (assoc :usage (assoc (:usage result) :model model)))))

(defn- prepare-generate-args
  "Validate options, extract tools/schema, build request opts. Returns a
   map of everything `generate` needs in either sync or async paths."
  [provider opts]
  (let [tools          (or (:tools opts) (:tools (:defaults provider)))
        schema         (or (:schema opts) (:schema (:defaults provider)))
        _              (when (and tools schema)
                         (throw (ex-info "Cannot use :tools and :schema simultaneously"
                                         {:error-type :llm/invalid-request})))
        model          (or (:model opts) (:model (:defaults provider)))
        on-text        (:on-text opts)
        on-tool-calls  (:on-tool-calls opts)
        on-tool-result (:on-tool-result opts)
        on-reasoning   (:on-reasoning opts)
        input-schemas  (when tools (tools->input-schemas tools))
        api-opts       (cond-> (dissoc opts :on-text :on-tool-calls :on-tool-result :on-reasoning)
                         tools (-> (dissoc :tools) (assoc :tools input-schemas)))
        name->fn       (when tools (build-name->fn tools input-schemas))]
    {:tools tools :schema schema :model model
     :on-text on-text :on-reasoning on-reasoning
     :on-tool-calls on-tool-calls :on-tool-result on-tool-result
     :input-schemas input-schemas :api-opts api-opts
     :name->fn name->fn}))

#?(:clj
   (defn generate
     "Blocking generation (Clojure / JVM). For ClojureScript use `generate-promise`,
      or for non-blocking JVM use `generate-ch`.

      (generate ai \"hello\")
      ;; => {:text \"Hello!\" :usage {:prompt-tokens 5 :completion-tokens 10} :timings {...}}

      (generate ai {:schema person-schema} \"extract this\")
      ;; => {:text \"{...}\" :structured {:name \"Alice\" :age 30} :usage {...}}

      (generate ai {:tools [#'get-weather]} \"Weather in Tokyo?\")
      ;; => {:text nil
      ;;     :tool-calls [{:id \"call_1\" :name \"get_weather\" :arguments {:city \"Tokyo\"}}]
      ;;     :tool-results [\"Sunny, 22C in Tokyo\"]
      ;;     :usage {...}}

      Core options:
        :model           - model name string
        :system-prompt   - system message string
        :schema          - Malli schema for structured output (uses tool-mode internally)
        :tools           - vector of tool vars (functions with Malli :=> metadata)
        :tool-choice     - \"none\" | \"auto\" | \"required\" | {:type \"function\" :function {:name ...}}
        :temperature     - float, e.g. 0.7
        :max-tokens      - int, max tokens to generate
        :top-p           - float, nucleus sampling

      Reasoning / thinking-mode (DeepSeek V4 Pro, OpenAI o-series, Gemini 3.x):
        :thinking         - map controlling thinking-mode, e.g. {:type \"enabled\"} or {:type \"disabled\"}
                            (DeepSeek). Pass-through, snake-cased.
        :reasoning-effort - \"high\" | \"max\" (DeepSeek), \"low\" | \"medium\" | \"high\" (OpenAI).

      Output shaping:
        :response-format  - raw provider response_format, e.g. {:type \"json_object\"} for JSON
                            mode. For Malli-driven structured output, prefer :schema instead.
                            Both can coexist (schema uses tool-mode under the hood).

      Reliability:
        :retry           - retry config for transient HTTP errors (429, 408, 5xx) and provider
                           mid-stream pressure signals (DeepSeek's insufficient_system_resource
                           finish_reason). Defaults to
                             {:max-attempts 3 :base-delay-ms 500 :max-delay-ms 30000}
                           Retries fire ONLY before any event has been forwarded to the
                           consumer — once a chunk has been emitted the stream is considered
                           committed and later errors flow through. Pass {:max-attempts 1} to
                           disable.

      Escape hatch:
        :provider-opts   - map of additional provider-specific API params. Kebab-case keys are
                           snake-cased before sending. Wins over the first-class options above
                           when keys collide.

      Callbacks (all optional, all called from a background thread / goroutine):
        :on-text         - (fn [chunk] ...) called for each text chunk as it streams
        :on-reasoning    - (fn [chunk] ...) called for each reasoning_content chunk (DeepSeek
                           thinking-mode, OpenRouter \"reasoning\" channel)
        :on-tool-calls   - (fn [{:keys [tool-calls text]}] ...) called before tools execute
        :on-tool-result  - (fn [{:keys [tool-call result error]}] ...) called after each tool

      Input is last — string, message-history vector, mixed content parts vector (for
      multimodal — see co.poyo.clj-llm.content), or a result map from a previous call.
      Result maps auto-unwrap :text when chained.

      Results are plain maps. Always present: :timings. Typically present: :text, :usage.
      Conditional: :reasoning (when the model emitted thinking content), :structured (when
      :schema was used), :tool-calls + :tool-results (when :tools were used)."
     ([provider input]
      (generate provider {} input))
     ([provider opts input]
      (let [start-time (now-ms)
            {:keys [tools schema model on-text on-reasoning
                    on-tool-calls on-tool-result
                    input-schemas api-opts name->fn]}
            (prepare-generate-args provider opts)

            timings  (atom {})
            result   (finalize-state
                      (chan-reduce (generate-reducer
                                    {:on-text on-text :on-reasoning on-reasoning
                                     :timings timings})
                                   init-state
                                   (events provider api-opts input)))
            end-time (now-ms)
            base     (build-base {:start-time start-time :end-time end-time
                                  :timings timings :model model :result result})]
        (finalize-with-tools-and-schema
         {:tools tools :schema schema :input-schemas input-schemas
          :name->fn name->fn :on-tool-calls on-tool-calls
          :on-tool-result on-tool-result :base base}
         result)))))

(defn generate-ch
  "Cross-platform generation returning a core.async channel.

   The channel yields exactly one value: the result map (same shape as
   `generate`) on success, or a Throwable/ex-info on failure.

   Available on both Clojure and ClojureScript. JVM callers typically
   prefer the blocking `generate`; CLJS callers prefer `generate-promise`."
  ([provider input] (generate-ch provider {} input))
  ([provider opts input]
   (let [start-time (now-ms)
         {:keys [tools schema model on-text on-reasoning
                 on-tool-calls on-tool-result
                 input-schemas api-opts name->fn]}
         (prepare-generate-args provider opts)

         timings  (atom {})
         reduced-ch (chan-reduce-ch (generate-reducer
                                     {:on-text on-text :on-reasoning on-reasoning
                                      :timings timings})
                                    init-state
                                    (events provider api-opts input))
         out (a/chan 1)]
     (a/go
       (let [v (a/<! reduced-ch)]
         (cond
           (nil? v)       (a/close! out)
           (throwable? v) (do (a/>! out v) (a/close! out))
           :else
           (let [end-time (now-ms)
                 v' (try
                      (let [result   (finalize-state v)
                            base     (build-base {:start-time start-time :end-time end-time
                                                  :timings timings :model model
                                                  :result result})]
                        (finalize-with-tools-and-schema
                         {:tools tools :schema schema :input-schemas input-schemas
                          :name->fn name->fn :on-tool-calls on-tool-calls
                          :on-tool-result on-tool-result :base base}
                         result))
                      (catch #?(:clj Exception :cljs :default) e e))]
             (a/>! out v')
             (a/close! out)))))
     out)))

#?(:cljs
   (defn generate-promise
     "Promise-returning generation for ClojureScript / Node.

      Returns a JavaScript Promise that resolves to the result map (same
      shape as JVM `generate`) or rejects with an Error/ex-info.

      (-> (generate-promise ai \"hello\")
          (.then (fn [r] (println (:text r))))
          (.catch (fn [e] (println \"Error:\" (.-message e)))))

      Options and result shape are identical to JVM `generate`. See its
      docstring for the full reference."
     ([provider input]
      (generate-promise provider {} input))
     ([provider opts input]
      (js/Promise.
       (fn [resolve reject]
         (let [ch (generate-ch provider opts input)]
           (a/go
             (let [v (a/<! ch)]
               (cond
                 (nil? v)       (resolve nil)
                 (throwable? v) (reject v)
                 :else          (resolve v))))))))))

;; ════════════════════════════════════════════════════════════════════
;; run-agent — JVM (blocking) and CLJS (Promise)
;; ════════════════════════════════════════════════════════════════════

(defn- agent-step
  "One iteration of the agent loop. Returns either:
   - a 'final' marker map {:done <result>} when we should stop
   - or a 'recur' marker map {:next <args>}  describing the next loop state

   Pure with respect to its argument; side-effects flow through callbacks."
  [{:keys [history steps step total-usage
           tools name->fn max-steps stop-when
           on-tool-calls on-tool-result]} result]
  (let [{:keys [text usage reasoning]} result
        parsed-calls (or (parse-tool-calls (:tool-calls result)) [])
        acc-usage (if (seq usage)
                    (merge-with (fn [a b] (if (and (number? a) (number? b)) (+ a b) b))
                                total-usage usage)
                    total-usage)
        _ (when (and on-tool-calls (seq parsed-calls))
            (on-tool-calls {:step step :tool-calls parsed-calls :text (not-empty text)}))
        pre-stop? (stop-when {:tool-calls parsed-calls :text text :step step})]
    (if pre-stop?
      {:done (cond-> {:text       text
                      :history    (conj history {:role :assistant :content (or text "")})
                      :steps      steps
                      :tool-calls (not-empty parsed-calls)}
               (seq acc-usage) (assoc :usage acc-usage))}

      (let [results (when (seq parsed-calls)
                      (mapv (fn [tc]
                              (let [tool-exec (try
                                                {:call tc :result (execute-tool-call name->fn tc)}
                                                (catch #?(:clj Exception :cljs :default) e
                                                  {:call tc :result (str "Error: " (err-msg e)) :error e}))]
                                (when on-tool-result
                                  (on-tool-result {:step step
                                                   :tool-call tc
                                                   :result (:result tool-exec)
                                                   :error (:error tool-exec)}))
                                tool-exec))
                            parsed-calls))
            tool-results (mapv :result results)
            assistant-msg (if results
                            (tool-calls->assistant-message parsed-calls text reasoning)
                            {:role :assistant :content (or text "")})
            tool-msgs    (if results
                           (mapv (fn [{:keys [call result]}]
                                   (tool-result (:id call) result))
                                 results)
                           [])
            next-history (-> history
                             (into [assistant-msg])
                             (into tool-msgs))
            next-steps   (if results
                           (conj steps {:tool-calls   (vec parsed-calls)
                                        :tool-results tool-results})
                           steps)
            post-stop? (when results
                         (stop-when {:tool-calls parsed-calls :text text :step step :tool-results tool-results}))]
        (if (or post-stop? (>= (inc step) max-steps))
          {:done (cond-> {:text text :history next-history :steps next-steps}
                   post-stop? (assoc :tool-calls (not-empty parsed-calls)
                                     :tool-results (not-empty tool-results))
                   (and (not post-stop?) (>= (inc step) max-steps)) (assoc :truncated true)
                   (seq acc-usage) (assoc :usage acc-usage))}
          {:next {:history next-history :steps next-steps
                  :step (inc step) :total-usage acc-usage}})))))

(defn- prepare-agent-args [provider opts]
  (let [parsed (parse-opts opts agent-opts-schema)
        tools  (or (:tools parsed) (:tools (:defaults provider)))
        _      (when-not (and (sequential? tools) (seq tools))
                 (throw (ex-info "run-agent requires a non-empty :tools vector"
                                 {:error-type :llm/invalid-request
                                  :tools tools})))
        input-schemas (tools->input-schemas tools)
        name->fn   (build-name->fn tools input-schemas)
        max-steps  (or (:max-steps parsed) 10)
        stop-when  (or (:stop-when parsed)
                       (fn [{:keys [tool-calls]}] (empty? tool-calls)))
        on-tool-calls  (:on-tool-calls parsed)
        on-tool-result (:on-tool-result parsed)
        on-text        (:on-text parsed)
        on-reasoning   (:on-reasoning parsed)
        request-opts (-> (dissoc parsed :max-steps :stop-when :on-tool-calls :on-tool-result :on-text :on-reasoning :tools)
                         (assoc :tools input-schemas))]
    {:tools tools :input-schemas input-schemas :name->fn name->fn
     :max-steps max-steps :stop-when stop-when
     :on-tool-calls on-tool-calls :on-tool-result on-tool-result
     :on-text on-text :on-reasoning on-reasoning
     :request-opts request-opts}))

#?(:clj
   (defn run-agent
     "Run an agentic tool-calling loop (Clojure / JVM blocking).
      For ClojureScript use `run-agent-promise`.

      Tools are plain functions with Malli function schemas attached via metadata
      ({:malli/schema [:=> [:cat <input-map>] <return>]}). The loop alternates
      provider calls with tool execution until the model stops calling tools or
      :max-steps is reached.

      (run-agent ai {:tools [#'get-weather]} \"Weather in Tokyo?\")
      (run-agent ai {:tools [#'get-weather #'search] :max-steps 5} \"plan a trip\")

      Agent-specific options (in addition to all `generate` options):
        :tools     - REQUIRED non-empty vector of tool vars
        :max-steps - max loop iterations, default 10. On exhaustion the result
                     is returned with :truncated true.
        :stop-when - (fn [{:keys [tool-calls text step tool-results]}] ...) →
                     bool. Default: stop when no tool calls in this turn.

      Reasoning round-trip: when the underlying model emits :reasoning-content
      (DeepSeek thinking-mode), it is preserved on the assistant turn going
      back to the API. Required by DeepSeek's tool-call thinking contract;
      transparent on other backends.

      Returns:
        {:text <final-assistant-text>
         :history [...]       — full message history including tool results
         :steps [{...}]       — per-step :tool-calls + :tool-results
         :tool-calls [...]    — last turn's tool calls (if any)
         :usage {...}         — summed across all turns
         :truncated true}     — present only when :max-steps was hit"
     ([provider input]
      (run-agent provider {} input))
     ([provider opts input]
      (let [{:keys [name->fn max-steps stop-when
                    on-tool-calls on-tool-result on-text on-reasoning
                    request-opts]} (prepare-agent-args provider opts)]
        (loop [history (build-messages input)
               steps []
               step 0
               total-usage {}]
          (let [result (finalize-state
                        (chan-reduce (fn [state event]
                                       (when (and on-text (= :content (:type event)))
                                         (on-text (:content event)))
                                       (when (and on-reasoning (= :reasoning (:type event)))
                                         (on-reasoning (:content event)))
                                       (next-state state event))
                                     init-state
                                     (events provider request-opts history)))
                outcome (agent-step {:history history :steps steps :step step
                                     :total-usage total-usage
                                     :name->fn name->fn :max-steps max-steps
                                     :stop-when stop-when
                                     :on-tool-calls on-tool-calls
                                     :on-tool-result on-tool-result}
                                    result)]
            (if (:done outcome)
              (:done outcome)
              (let [{:keys [history steps step total-usage]} (:next outcome)]
                (recur history steps step total-usage)))))))))

(defn run-agent-ch
  "Cross-platform agent loop returning a core.async channel.

   The channel yields exactly one value: the result map (same shape as
   `run-agent`) on success, or a Throwable/ex-info on failure."
  ([provider input] (run-agent-ch provider {} input))
  ([provider opts input]
   (let [{:keys [name->fn max-steps stop-when
                 on-tool-calls on-tool-result on-text on-reasoning
                 request-opts]} (prepare-agent-args provider opts)
         out (a/chan 1)]
     (a/go
       (loop [history (build-messages input)
              steps []
              step 0
              total-usage {}]
         (let [reduced-ch (chan-reduce-ch (fn [state event]
                                            (when (and on-text (= :content (:type event)))
                                              (on-text (:content event)))
                                            (when (and on-reasoning (= :reasoning (:type event)))
                                              (on-reasoning (:content event)))
                                            (next-state state event))
                                          init-state
                                          (events provider request-opts history))
               raw (a/<! reduced-ch)]
           (cond
             (throwable? raw)
             (do (a/>! out raw) (a/close! out))

             :else
             (let [result-or-err (try
                                   (let [result (finalize-state raw)
                                         outcome (agent-step
                                                  {:history history :steps steps :step step
                                                   :total-usage total-usage
                                                   :name->fn name->fn :max-steps max-steps
                                                   :stop-when stop-when
                                                   :on-tool-calls on-tool-calls
                                                   :on-tool-result on-tool-result}
                                                  result)]
                                     outcome)
                                   (catch #?(:clj Exception :cljs :default) e e))]
               (cond
                 (throwable? result-or-err)
                 (do (a/>! out result-or-err) (a/close! out))

                 (:done result-or-err)
                 (do (a/>! out (:done result-or-err)) (a/close! out))

                 :else
                 (let [{:keys [history steps step total-usage]} (:next result-or-err)]
                   (recur history steps step total-usage))))))))
     out)))

#?(:cljs
   (defn run-agent-promise
     "Promise-returning agent loop for ClojureScript / Node."
     ([provider input]
      (run-agent-promise provider {} input))
     ([provider opts input]
      (js/Promise.
       (fn [resolve reject]
         (let [ch (run-agent-ch provider opts input)]
           (a/go
             (let [v (a/<! ch)]
               (cond
                 (nil? v)       (resolve nil)
                 (throwable? v) (reject v)
                 :else          (resolve v))))))))))
