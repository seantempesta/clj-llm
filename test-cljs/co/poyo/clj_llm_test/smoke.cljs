(ns co.poyo.clj-llm-test.smoke
  "Pure-CLJS smoke test — no network. Verifies the protocol/state-machine/
   schema layer compiles and runs in Node."
  (:require
   [co.poyo.clj-llm.core :as llm]
   [co.poyo.clj-llm.schema :as schema]
   [co.poyo.clj-llm.stream :as stream]
   [co.poyo.clj-llm.backend.openai :as openai]
   [malli.core :as m]))

(defn- check [label actual expected]
  (let [ok? (= actual expected)]
    (println (str (if ok? "ok  " "FAIL") "  " label
                  (when-not ok? (str "  (got " (pr-str actual)
                                     " expected " (pr-str expected) ")"))))
    ok?))

(defn -main []
  (println "=== clj-llm CLJS smoke test ===")
  (let [ok (atom true)
        track! (fn [b] (when-not b (reset! ok false)))]

    (track! (check "sse parse skips non-data"
              (stream/parse-sse-data ": ping") nil))

    (track! (check "sse parse [DONE]"
              (stream/parse-sse-data "data: [DONE]") nil))

    (track! (check "sse parse simple json"
              (stream/parse-sse-data "data: {\"a\": 1}")
              {:a 1}))

    (track! (check "sse parse with camel keys → kebab"
              (stream/parse-sse-data "data: {\"finishReason\": \"stop\"}")
              {:finish-reason "stop"}))

    (track! (check "malli→json-schema simple"
              (schema/malli->json-schema [:map [:a :int] [:b :string]])
              {:type "object"
               :properties {"a" {:type "integer"}
                            "b" {:type "string"}}
               :required ["a" "b"]}))

    (track! (check "backend constructor returns a record"
              (some? (openai/backend {:api-key "sk-fake"
                                      :api-base "http://example.invalid"
                                      :defaults {:model "x"}}))
              true))

    (track! (check "generate-promise exists"
              (fn? llm/generate-promise) true))
    (track! (check "run-agent-promise exists"
              (fn? llm/run-agent-promise) true))

    (if @ok
      (do (println "\nSMOKE: PASS") (js/process.exit 0))
      (do (println "\nSMOKE: FAIL") (js/process.exit 1)))))
