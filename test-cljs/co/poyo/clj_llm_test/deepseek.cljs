(ns co.poyo.clj-llm-test.deepseek
  "End-to-end CLJS test of the clj-llm port against DeepSeek's
   OpenAI-compatible API. Mirrors the JVM Phase-1 tests:

   - chat completion against deepseek-v4-flash
   - chat completion against deepseek-v4-pro
   - streaming via :on-text
   - tool calling with a Malli-spec'd add tool

   Reads DEEPSEEK_API_KEY from process.env."
  (:require
   [co.poyo.clj-llm.core :as llm]
   [co.poyo.clj-llm.backend.openai :as openai]
   [clojure.string :as str]
   [malli.core :as m]))

(def ^:private api-key
  (some-> js/process .-env .-DEEPSEEK_API_KEY))

(defn- make-backend [model]
  (openai/backend {:api-key  api-key
                   :api-base "https://api.deepseek.com/v1"
                   :defaults {:model model}}))

(defn- summarize [label r]
  (println (str label "-TEXT-LEN: " (count (:text r))))
  (println (str label "-FIRST-CHARS: " (apply str (take 20 (or (:text r) "")))))
  (when-let [u (:usage r)]
    (println (str label "-TOKENS: total=" (:total-tokens u)
                  " prompt=" (:prompt-tokens u)
                  " completion=" (:completion-tokens u)
                  " model=" (:model u)))))

(defn- run-flash []
  (println "=== FLASH chat completion ===")
  (-> (llm/generate-promise (make-backend "deepseek-v4-flash")
                            "Say hello in one short sentence.")
      (.then (fn [r]
               (summarize "FLASH" r)
               r))))

(defn- run-pro []
  (println "\n=== PRO chat completion ===")
  (-> (llm/generate-promise (make-backend "deepseek-v4-pro")
                            "Say hello in one short sentence.")
      (.then (fn [r]
               (summarize "PRO" r)
               r))))

(defn- run-streaming []
  (println "\n=== STREAM test (deepseek-v4-flash) ===")
  (let [chunks (atom 0)]
    (-> (llm/generate-promise (make-backend "deepseek-v4-flash")
                              {:on-text (fn [_] (swap! chunks inc))}
                              "Say hello in one short sentence.")
        (.then (fn [r]
                 (println (str "STREAM-CHUNKS: " @chunks))
                 (println (str "STREAM-TEXT-LEN: " (count (:text r))))
                 r)))))

(def ^:private add-tool-schema
  [:=> [:cat [:map {:name "add"
                    :description "Add two integers."}
              [:a :int]
              [:b :int]]]
   :int])

;; CLJS fns don't carry meta by default; use a meta-bearing wrapper so the
;; existing `resolve-tool-schema` path (which looks at :malli/schema in meta)
;; finds the schema. Equivalent semantically to the JVM mx/defn pattern.
(defn make-add-tool []
  (with-meta
    (fn add [{:keys [a b]}] (+ a b))
    {:malli/schema add-tool-schema}))

(defn- run-tool []
  (println "\n=== TOOL CALL test (deepseek-v4-flash) ===")
  (-> (llm/generate-promise (make-backend "deepseek-v4-flash")
                            {:tools [(make-add-tool)]}
                            "Use the add tool to compute 17+25.")
      (.then (fn [r]
               (println (str "TOOL-CALLS-COUNT: " (count (:tool-calls r))))
               (when-let [tc (first (:tool-calls r))]
                 (println (str "TOOL-NAME: " (:name tc)))
                 (println (str "TOOL-ARGS-IS-MAP: " (map? (:arguments tc))))
                 (println (str "TOOL-ARGS-KEYS: " (pr-str (sort (keys (:arguments tc)))))))
               (println (str "TOOL-RESULTS-COUNT: " (count (:tool-results r))))
               (println (str "TOOL-RESULT-FIRST: " (first (:tool-results r))))
               r))))

(defn -main []
  (when (str/blank? api-key)
    (println "ERROR: DEEPSEEK_API_KEY not set in environment.")
    (js/process.exit 1))
  (-> (.then (js/Promise.resolve) run-flash)
      (.then run-pro)
      (.then run-streaming)
      (.then run-tool)
      (.then (fn [_] (println "\nALL DEEPSEEK TESTS DONE")
                (js/process.exit 0)))
      (.catch (fn [e]
                (println "\nFATAL:" (or (.-message e) (str e)))
                (when-let [d (some-> (ex-data e) pr-str)]
                  (println "DATA:" d))
                (js/process.exit 1)))))
