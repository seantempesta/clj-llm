(ns co.poyo.clj-llm.coverage-gaps-test
  "Tests for coverage gaps 1-4 and 6 (Gap 5 — streaming-disable knob — deferred):
   1. Reasoning round-trip in tool mode (correctness bug, DeepSeek Pro-blocking)
   2. :thinking + :reasoning-effort first-class opts
   3. :response-format first-class opt
   4. insufficient_system_resource → retryable error
   6. HTTP 429 + transient 5xx retry"
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.core.async :as a]
   [malli.core :as m]
   [co.poyo.clj-llm.core :as llm]
   [co.poyo.clj-llm.protocol :as proto]
   [co.poyo.clj-llm.backend.openai :as openai]
   [co.poyo.clj-llm.stream :as stream]))

;; ════════════════════════════════════════════════════════════════════
;; Multi-turn mock provider — supports agent loops + retry attempts
;; ════════════════════════════════════════════════════════════════════

(defrecord MultiTurnMockProvider [turns calls defaults]
  proto/LLMProvider
  (api-key [_] "mock-key")
  (build-url [_ model] (str "https://mock/" model))
  (build-headers [_] {})
  (build-body [_ model system-prompt messages schema tools tool-choice provider-opts]
    (swap! calls conj {:model model
                       :system-prompt system-prompt
                       :messages messages
                       :schema schema
                       :tools tools
                       :tool-choice tool-choice
                       :provider-opts provider-opts})
    {:model model :messages messages})
  (parse-chunk [_ chunk _schema _tools]
    (if (:type chunk) [chunk] []))
  (stream-events [_ _url _headers _body]
    (let [[old _] (swap-vals! turns rest)
          turn    (first old)
          ch      (a/chan 256)]
      (a/thread
        (doseq [e (vec turn)]
          (a/>!! ch e))
        (a/close! ch))
      ch)))

(defn multi-turn-mock
  ([turns] (multi-turn-mock turns {}))
  ([turns defaults]
   (->MultiTurnMockProvider (atom (vec turns))
                            (atom [])
                            (merge {:model "test-model"} defaults))))

;; ════════════════════════════════════════════════════════════════════
;; Gap 1 — reasoning round-trip in tool-call multi-turn
;; ════════════════════════════════════════════════════════════════════

(defn get-weather
  {:malli/schema [:=> [:cat [:map {:name "get_weather"} [:city :string]]] :string]}
  [{:keys [city]}]
  (str "Sunny in " city))

(deftest gap-1-reasoning-round-trips-via-run-agent
  (testing "run-agent preserves :reasoning-content on the assistant turn when tools were called"
    (let [provider (multi-turn-mock
                    [;; turn 1: reasoning + tool call
                     [{:type :reasoning :content "Thinking about the weather..."}
                      {:type :tool-call :id "call_1" :index 0
                       :name "get_weather" :arguments "{\"city\":\"Tokyo\"}"}
                      {:type :finish :reason "tool_calls"}]
                     ;; turn 2: final text
                     [{:type :content :content "Tokyo is sunny."}
                      {:type :finish :reason "stop"}]])
          result (llm/run-agent provider {:tools [#'get-weather] :max-steps 3}
                                "weather in Tokyo?")
          calls  @(:calls provider)]
      (is (= 2 (count calls)) "Agent made two provider calls")
      (let [turn-2-messages (-> calls second :messages)
            assistant-msg   (first (filter #(= :assistant (:role %)) turn-2-messages))]
        (is (some? assistant-msg) "Turn 2 has an assistant message in history")
        (is (= "Thinking about the weather..." (:reasoning-content assistant-msg))
            "Reasoning-content from turn 1 is round-tripped into turn 2's history"))
      (is (= "Tokyo is sunny." (:text result))))))

(deftest gap-1-no-reasoning-content-when-empty
  (testing "Assistant message has no :reasoning-content key when no reasoning was emitted"
    (let [provider (multi-turn-mock
                    [;; turn 1: tool call only, no reasoning
                     [{:type :tool-call :id "c1" :index 0
                       :name "get_weather" :arguments "{\"city\":\"NYC\"}"}
                      {:type :finish :reason "tool_calls"}]
                     ;; turn 2: text
                     [{:type :content :content "NYC: sunny"}
                      {:type :finish :reason "stop"}]])
          _result   (llm/run-agent provider {:tools [#'get-weather]} "NYC weather?")
          turn-2    (-> @(:calls provider) second :messages)
          assistant (first (filter #(= :assistant (:role %)) turn-2))]
      (is (not (contains? assistant :reasoning-content))
          "No :reasoning-content key when reasoning is empty"))))

(deftest gap-1-openai-normalize-renames-reasoning-content
  (testing "openai backend renames :reasoning-content → :reasoning_content"
    (let [normalize @#'co.poyo.clj-llm.backend.openai/normalize-messages
          input  [{:role :assistant
                   :content "hello"
                   :reasoning-content "thought"
                   :tool-calls [{:id "x" :type "function"
                                 :function {:name "f" :arguments "{}"}}]}]
          output (normalize input)
          msg    (first output)]
      (is (= "thought" (:reasoning_content msg)))
      (is (not (contains? msg :reasoning-content)))
      (is (some? (:tool_calls msg))
          "tool-calls rename still works (regression check)"))))

;; ════════════════════════════════════════════════════════════════════
;; Gap 2 — :thinking + :reasoning-effort
;; ════════════════════════════════════════════════════════════════════

(deftest gap-2-thinking-and-reasoning-effort-lift-into-provider-opts
  (testing ":thinking and :reasoning-effort are lifted into provider-opts"
    (let [provider (multi-turn-mock
                    [[{:type :content :content "ok"}
                      {:type :finish :reason "stop"}]])]
      (llm/generate provider
                    {:thinking {:type "enabled"}
                     :reasoning-effort "high"}
                    "ping")
      (let [opts (:provider-opts (first @(:calls provider)))]
        (is (= {:type "enabled"} (:thinking opts)))
        (is (= "high" (:reasoning-effort opts)))))))

(deftest gap-2-thinking-and-reasoning-effort-snake-case-via-openai
  (testing "Snake-case conversion in openai backend body"
    (let [build-body @#'co.poyo.clj-llm.backend.openai/build-body-internal
          body (build-body "deepseek-v4-pro"
                           nil
                           [{:role "user" :content "hi"}]
                           nil nil nil
                           {:thinking {:type "enabled"}
                            :reasoning-effort "high"})]
      (is (= {:type "enabled"} (:thinking body)))
      (is (= "high" (:reasoning_effort body)))
      (is (not (contains? body :reasoning-effort))))))

;; ════════════════════════════════════════════════════════════════════
;; Gap 3 — :response-format
;; ════════════════════════════════════════════════════════════════════

(deftest gap-3-response-format-lifts-and-snake-cases
  (testing ":response-format flows through and snake-cases"
    (let [build-body @#'co.poyo.clj-llm.backend.openai/build-body-internal
          body (build-body "deepseek-v4-flash"
                           nil
                           [{:role "user" :content "give me JSON"}]
                           nil nil nil
                           {:response-format {:type "json_object"}})]
      (is (= {:type "json_object"} (:response_format body)))
      (is (not (contains? body :response-format))))))

;; ════════════════════════════════════════════════════════════════════
;; Gap 4 — insufficient_system_resource
;; ════════════════════════════════════════════════════════════════════

(deftest gap-4-openai-emits-retryable-error-on-insufficient-system-resource
  (testing "data->events converts insufficient_system_resource finish_reason to retryable :error"
    (let [data->events @#'co.poyo.clj-llm.backend.openai/data->events
          events (data->events {:choices [{:finish-reason "insufficient_system_resource"}]
                                :usage {:prompt-tokens 5 :completion-tokens 0}}
                               nil nil)
          [err usage] events]
      (is (= :error (:type err)))
      (is (true? (get-in err [:error :retryable?])))
      (is (true? (get-in err [:error :transient?])))
      (is (= "insufficient_system_resource" (get-in err [:error :finish-reason])))
      (is (= :usage (:type usage)) "Usage event still flows"))))

(deftest gap-4-normal-finish-reason-not-retryable
  (testing "Normal finish_reason emits :finish (not :error)"
    (let [data->events @#'co.poyo.clj-llm.backend.openai/data->events
          events (data->events {:choices [{:finish-reason "stop"}]
                                :usage {:prompt-tokens 5 :completion-tokens 10}}
                               nil nil)]
      (is (= :finish (:type (first events))))
      (is (= "stop" (:reason (first events)))))))

(deftest gap-4-retry-on-retryable-error-event
  (testing "Retry wrapper retries on :error event with :retryable? true, before any commit"
    (let [provider (multi-turn-mock
                    [;; turn 1: retryable error before any content
                     [{:type :error :error {:message "backend pressure" :retryable? true}}]
                     ;; turn 2: success
                     [{:type :content :content "back online"}
                      {:type :finish :reason "stop"}]])
          result (llm/generate provider
                               {:retry {:max-attempts 3 :base-delay-ms 5 :max-delay-ms 50}}
                               "test")]
      (is (= "back online" (:text result)))
      (is (= 2 (count @(:calls provider))) "Retry triggered exactly one extra call"))))

(deftest gap-4-no-retry-once-content-committed
  (testing "Errors after content has been emitted are not retried (commit point reached)"
    (let [provider (multi-turn-mock
                    [;; turn 1: content emitted, then a retryable error
                     [{:type :content :content "partial..."}
                      {:type :error :error {:message "boom" :retryable? true}}]])
          thrown (try (llm/generate provider
                                    {:retry {:max-attempts 5 :base-delay-ms 5 :max-delay-ms 50}}
                                    "test")
                      (catch Exception e e))]
      (is (instance? Exception thrown) "Error surfaces — retry was not safe")
      (is (= 1 (count @(:calls provider))) "Exactly one attempt"))))

;; ════════════════════════════════════════════════════════════════════
;; Gap 6 — HTTP 429 + transient 5xx retry
;; ════════════════════════════════════════════════════════════════════

(deftest gap-6-retry-on-retryable-throwable
  (testing "Throwable with :retryable? true triggers retry"
    (let [provider (multi-turn-mock
                    [[(ex-info "HTTP 429" {:status 429 :retryable? true})]
                     [{:type :content :content "throttle cleared"}
                      {:type :finish :reason "stop"}]])
          result (llm/generate provider
                               {:retry {:max-attempts 3 :base-delay-ms 5 :max-delay-ms 50}}
                               "test")]
      (is (= "throttle cleared" (:text result)))
      (is (= 2 (count @(:calls provider)))))))

(deftest gap-6-no-retry-on-non-retryable-throwable
  (testing "Throwable without :retryable? is surfaced immediately"
    (let [provider (multi-turn-mock
                    [[(ex-info "HTTP 400 Bad Request" {:status 400})]])
          thrown (try (llm/generate provider
                                    {:retry {:max-attempts 3 :base-delay-ms 5 :max-delay-ms 50}}
                                    "test")
                      (catch Exception e e))]
      (is (instance? Exception thrown))
      (is (= 1 (count @(:calls provider))) "No retry on non-retryable error"))))

(deftest gap-6-retry-budget-exhausted
  (testing "After max-attempts retries, the final error surfaces"
    (let [provider (multi-turn-mock
                    (repeat 5 [(ex-info "HTTP 503" {:status 503 :retryable? true})]))
          thrown (try (llm/generate provider
                                    {:retry {:max-attempts 3 :base-delay-ms 5 :max-delay-ms 50}}
                                    "test")
                      (catch Exception e e))]
      (is (instance? Exception thrown))
      (is (= 3 (count @(:calls provider))) "Exactly max-attempts attempts"))))

(deftest gap-6-stream-retryable-statuses
  (testing "stream/retryable-statuses includes 408, 429, and transient 5xx"
    (let [retryable @#'co.poyo.clj-llm.stream/retryable-statuses]
      (is (every? retryable [408 429 500 502 503 504]))
      (is (not (retryable 200)))
      (is (not (retryable 400)))
      (is (not (retryable 401)))
      (is (not (retryable 404))))))

;; ════════════════════════════════════════════════════════════════════
;; Retry — backoff math
;; ════════════════════════════════════════════════════════════════════

(deftest retry-backoff-doubles-and-caps
  (let [backoff @#'co.poyo.clj-llm.core/compute-backoff]
    (is (= 500   (backoff 500 30000 1))  "attempt 1 → base")
    (is (= 1000  (backoff 500 30000 2))  "attempt 2 → 2× base")
    (is (= 2000  (backoff 500 30000 3))  "attempt 3 → 4× base")
    (is (= 4000  (backoff 500 30000 4))  "attempt 4 → 8× base")
    (is (= 30000 (backoff 500 30000 20)) "attempt 20 → capped at max")))
