# clj-llm

> **Fork notice.** This is a personal fork of [minikomi/clj-llm](https://github.com/minikomi/clj-llm). It adds a ClojureScript / Node port: the source tree is single-`.cljc`, the CLJS HTTP transport uses native `js/fetch` (Node 18+), and `generate-promise` / `generate-ch` / `run-agent-promise` provide async entry points for JS-hosted runtimes. License unchanged: EPL-1.0 (see `LICENSE`).

A Clojure library for talking to LLMs. Providers are plain maps. Results are plain maps. Everything composes with standard Clojure.

## Installation

Upstream:

```clojure
{:deps {co.poyo/clj-llm {:git/url "https://github.com/minikomi/clj-llm"
                         :git/sha "..."}}}
```

This fork (CLJS port + Vertex backend + reasoning round-trip + retry layer):

```clojure
{:deps {co.poyo/clj-llm {:git/url "https://github.com/seantempesta/clj-llm"
                         :git/tag "v0.2.0-cljs-port"
                         :git/sha "..."}}}
```

## Quick start

```clojure
(require '[co.poyo.clj-llm.core :as llm]
         '[co.poyo.clj-llm.backend.openai :as openai])

(def ai (openai/backend {:api-key "sk-..."
                         :defaults {:model "gpt-4o-mini"}}))

(:text (llm/generate ai "What is the capital of France?"))
;; => "The capital of France is Paris."
```

## Overview

| Concept | How |
|---|---|
| Provider | `(openai/backend)` — a map |
| Config | `(assoc provider :defaults {...})` |
| Text | `(generate ai "prompt")` → `{:text "..." :usage {...}}` |
| Options | `(generate ai {:system-prompt "..."} "prompt")` |
| Structured | `(generate ai {:schema s} "prompt")` → `{:structured {...} ...}` |
| Tool call | `(generate ai {:tools [...]} "prompt")` → `{:tool-calls [...] :tool-results [...]}` |
| Agent loop | `(run-agent ai {:tools [#'tool]} "prompt")` → `{:text ... :steps ... :history ...}` |
| Streaming | `(generate ai {:on-text print} "prompt")` |
| Raw events | `(events ai "prompt")` → core.async channel |
| Images/PDFs | `(generate ai ["describe" (content/image "photo.jpg")])` |
| Chaining | `(->> "text" (generate ai) (generate ai))` |

## Providers

A provider is just a map. Put defaults on `:defaults`:

```clojure
(def openai  (openai/backend {:api-key "sk-..."}))
(def claude  (anthropic/backend {:api-key "sk-ant-..."}))
(def ollama  (openai/backend {:api-base "http://localhost:11434/v1"
                              :api-key false}))
(def router  (openai/backend {:api-key "sk-..."
                              :api-base "https://openrouter.ai/api/v1"}))

(def ai (openai/backend {:api-key "sk-..."
                         :defaults {:model "gpt-4o-mini"}}))

;; Layer more config with standard map ops
(def careful (update ai :defaults merge {:model "gpt-4o" :temperature 0.2}))
```

`api-key` can be a string, a zero-arg function (called on every request — useful for token refresh), or `false`.

When no `:api-key` is provided, each backend reads from a default environment variable:

| Backend | Constructor | Env var | Notes |
|---|---|---|---|
| OpenAI | `openai/backend` | `OPENAI_API_KEY` | Also works for OpenAI-compatible providers (DeepSeek, OpenRouter, Together, Groq, Ollama, etc.) by overriding `:api-base` |
| Anthropic | `anthropic/backend` | `ANTHROPIC_API_KEY` | Native Anthropic API |
| OpenRouter | `openrouter/backend` | `OPENROUTER_API_KEY` | Native OpenRouter; for plain OpenAI-compat use `openai/backend` with OpenRouter's URL |
| Vertex (Gemini) | `vertex/backend` (JVM only) | n/a — uses GCP ADC | See the Vertex section below |

### DeepSeek

Use the OpenAI backend with DeepSeek's base URL. The library handles DeepSeek's thinking-mode round-trip in agent loops correctly (reasoning content is preserved on the assistant turn — required by DeepSeek's contract).

```clojure
(def deepseek
  (openai/backend
    {:api-key  (System/getenv "DEEPSEEK_API_KEY")
     :api-base "https://api.deepseek.com/v1"
     :defaults {:model "deepseek-v4-flash"}}))   ; or "deepseek-v4-pro"

(llm/generate deepseek "Say hi")

;; Thinking-mode + reasoning effort (DeepSeek-specific knobs, first-class)
(llm/generate deepseek
              {:model "deepseek-v4-pro"
               :thinking {:type "enabled"}
               :reasoning-effort "high"}
              "Plan a refactor")
```

### Vertex / Gemini (JVM only)

Routes Gemini through Google Cloud Vertex AI's OpenAI-compatible endpoint with ADC bearer-token auth. Pick this over the public AI Studio endpoint when you can't have prompts used for training. Auth: `gcloud auth application-default login` once, then the library refreshes tokens automatically.

```clojure
(require '[co.poyo.clj-llm.backend.vertex :as vertex])

(def gemini
  (vertex/backend
    {:project "my-gcp-project"               ; or set GOOGLE_CLOUD_PROJECT
     :defaults {:model "gemini-3-flash-preview"}}))

;; Chat
(llm/generate gemini "Hello")

;; Vision with a GCS URI — no download/encode round-trip
(llm/generate gemini
              [(content/text "What's in this image?")
               (content/image "gs://my-bucket/photo.jpg")])

;; Embeddings (native :predict endpoint, separate from the chat protocol)
(vertex/embed gemini
              {:model     "text-embedding-005"     ; or "gemini-embedding-001"
               :inputs    ["doc 1" "doc 2"]
               :task-type "RETRIEVAL_DOCUMENT"
               :dimensions 768})
;; => {:embeddings [[…] […]] :stats [{:token_count 5} …] :usage {:total-tokens 11}}
```

Model names are bare Gemini IDs (`"gemini-3-pro-preview"`). The library adds the `google/` publisher prefix Vertex requires. `:location` defaults to `"global"` — required for Gemini 3.x preview models as of mid-2026.

## Generate

Input is always last. Options go before it:

```clojure
(llm/generate ai "hello")
;; => {:text "Hello!" :usage {:prompt-tokens 5 :completion-tokens 3}}

(llm/generate ai {:system-prompt "Answer in one word."} "Capital of France?")
;; => {:text "Paris." :usage {...}}

(llm/generate ai {:model         "gpt-4o"
                  :system-prompt "Be concise."
                  :temperature   0.2
                  :max-tokens    100}
  "Explain recursion")
```

Full first-class options:

| Option | Type | Notes |
|---|---|---|
| `:model` | string | model name |
| `:system-prompt` | string | system message |
| `:schema` | Malli schema | structured output via tool-mode (returns `:structured`) |
| `:tools` | vector of vars | tool-calling — see Tool calling section |
| `:tool-choice` | string/map | `"none"`, `"auto"`, `"required"`, or specific function |
| `:temperature` | float | |
| `:max-tokens` | int | |
| `:top-p` | float | |
| `:thinking` | map | DeepSeek thinking-mode, e.g. `{:type "enabled"}` |
| `:reasoning-effort` | string | DeepSeek `"high"`/`"max"`, OpenAI `"low"`/`"medium"`/`"high"` |
| `:response-format` | map | raw provider response_format, e.g. `{:type "json_object"}` for JSON mode. For Malli structured output prefer `:schema`. |
| `:retry` | map | retry config — see Retries below |
| `:provider-opts` | map | escape hatch for raw provider params (kebab-case is snake-cased automatically) |

Callbacks: `:on-text`, `:on-reasoning`, `:on-tool-calls`, `:on-tool-result`.

For provider-specific params not yet first-class: `:provider-opts {:frequency_penalty 0.5}` — keys are snake-cased before sending.

## Chaining

Results have `:text` and `:structured` — both auto-unwrap when passed as input:

```clojure
(->> "The mitochondria is the powerhouse of the cell. It make ATP."
     (llm/generate ai {:system-prompt "Fix grammar."})
     (llm/generate ai {:system-prompt "Translate to French."}))
;; => {:text "La mitochondrie est la centrale..." :usage {...}}
```

A result with `:structured` unwraps via `prn-str`.

## Structured output

Pass a Malli schema to get parsed, validated data:

```clojure
(llm/generate ai
  {:schema [:map [:name :string] [:age :int] [:occupation :string]]}
  "Marie Curie was a 66 year old physicist")
;; => {:text "{...}" :structured {:name "Marie Curie" :age 66 :occupation "physicist"} :usage {...}}
```

Build reusable extractors with `update`/`merge`:

```clojure
(def extractor
  (update ai :defaults merge
    {:system-prompt "Extract structured data."
     :schema [:map [:name :string] [:age :int] [:occupation :string]]}))

(:structured (llm/generate extractor "Albert Einstein was a 76 year old physicist"))
;; => {:name "Albert Einstein" :age 76 :occupation "theoretical physicist"}
```

## Streaming

`:on-text` streams chunks while still returning the full result:

```clojure
(llm/generate ai {:on-text (fn [chunk] (print chunk) (flush))} "Write a haiku")
;; prints live, then returns {:text "..." :usage {...}}
```

For reasoning models (o1, o3, etc.), `:on-reasoning` streams the model's internal reasoning:

```clojure
(llm/generate ai {:on-reasoning (fn [chunk] (print "[thinking]" chunk) (flush))
                  :on-text      (fn [chunk] (print chunk) (flush))}
  "Solve this logic puzzle: ...")
;; prints reasoning chunks with [thinking] prefix, then the final answer
```

## Conversations

Message history is a vector you pass as input:

```clojure
(def convo (atom []))

(defn chat! [msg]
  (swap! convo conj {:role :user :content msg})
  (let [{:keys [text]} (llm/generate ai @convo)]
    (swap! convo conj {:role :assistant :content text})
    text))

(chat! "What's the tallest mountain?")  ;; => "Mount Everest..."
(chat! "Second tallest?")               ;; => "K2..."
```

## Images and PDFs

```clojure
(require '[co.poyo.clj-llm.content :as content])

(:text (llm/generate ai ["What's in this image?" (content/image "photo.jpg")]))
(:text (llm/generate ai ["Describe this" (content/image "https://example.com/chart.png")]))
(:text (llm/generate claude-ai ["Summarize" (content/pdf "invoice.pdf")]))

;; Resize to control cost and size limits
(content/image "huge.jpg" {:max-edge 512})
(content/image "photo.png" {:max-edge 1024 :format "jpeg" :quality 85})
```

## Tool calling

Tools are plain functions with [Malli function schemas](https://github.com/metosin/malli/blob/master/docs/function-schemas.md):

```clojure
(defn get-weather
  {:malli/schema [:=> [:cat [:map {:name "get_weather"
                                   :description "Get current weather"}
                             [:city {:description "City name"} :string]]]
                      :string]}
  [{:keys [city]}]
  (str "Sunny, 22°C in " city))

;; Single LLM call — model decides whether to use tools
(llm/generate ai {:tools [#'get-weather]} "Weather in Tokyo?")
;; => {:text nil
;;     :tool-calls [{:id "call_1" :name "get_weather" :arguments {:city "Tokyo"}}]
;;     :tool-results ["Sunny, 22°C in Tokyo"]
;;     :usage {...}}

;; Agent loop — keeps calling tools until the model is done
(llm/run-agent ai {:tools [#'get-weather]} "Weather in Tokyo?")
;; => {:text    "It's currently sunny and 22°C in Tokyo."
;;     :history [...]
;;     :steps   [{:tool-calls [...] :tool-results [...]}]
;;     :usage   {...}}
```

All Malli schema styles work: `{:malli/schema ...}` metadata, `mx/defn`, `m/=>`.

### Agent options

```clojure
(llm/run-agent ai
  {:tools [#'search #'done]
   :max-steps  5
   :stop-when  (fn [{:keys [tool-calls]}]
                 (some #(= "done" (:name %)) tool-calls))
   :on-text        (fn [chunk] (print chunk) (flush))
   :on-reasoning   (fn [chunk] (print "[thinking]" chunk) (flush))
   :on-tool-calls  (fn [{:keys [step tool-calls]}]
                     (println "Step" step (mapv :name tool-calls)))
   :on-tool-result (fn [{:keys [tool-call result error]}]
                     (println " ->" (:name tool-call) result))}
  "Research quantum computing")
```

`:stop-when` fires before tools execute — pending calls are returned in `:tool-calls` without being run.

### Structured output after tool use

```clojure
(let [{:keys [history]} (llm/run-agent ai {:tools [#'lookup]} "Find user 123")]
  (:structured (llm/generate ai {:schema [:map [:name :string] [:status :string]]} history)))
```

## generate vs run-agent

| | `generate` | `run-agent` |
|---|---|---|
| LLM calls | Exactly one | Loop until done |
| Tools | Optional (`:tools` in opts) | Required (`:tools` in opts) |
| Stop control | N/A | `:stop-when`, `:max-steps` |
| Returns | `{:text :usage}` or `{:structured}` or `{:tool-calls :tool-results}` | `{:text :history :steps}` |

## Raw events

```clojure
(require '[clojure.core.async :refer [<!!]])

(let [ch (llm/events ai "Count to 5")]
  (loop []
    (when-let [event (<!! ch)]
      (println (:type event) (dissoc event :type))
      (recur))))
;; :content {:content "1"}
;; :usage {:prompt-tokens 10 :completion-tokens 20}
;; :done {}
```

Event types: `:content`, `:reasoning`, `:tool-call`, `:tool-call-delta`, `:usage`, `:finish`, `:error`, `:done`.

## Error handling

Connection errors throw plain Java exceptions. HTTP errors throw `ex-info` with `:status` and `:body`:

```clojure
(try
  (llm/generate ai {:model "nonexistent"} "hello")
  (catch clojure.lang.ExceptionInfo e
    (let [{:keys [status body]} (ex-data e)]
      (println "HTTP" status body)))
  (catch Exception e
    (println "Connection error:" (.getMessage e))))
```

Option validation errors are `ex-info` with `:error-type :llm/invalid-request`.

## Retries

The library auto-retries on transient failures: HTTP 408/429/5xx, connection errors, and provider mid-stream pressure signals like DeepSeek's `insufficient_system_resource` finish_reason.

Retries fire **only before any event has been forwarded to the consumer** — once a chunk has been emitted (e.g., a streaming `:on-text` callback fired), the stream is considered committed and later errors flow through normally.

```clojure
;; Default config — used automatically:
;;   {:max-attempts 3 :base-delay-ms 500 :max-delay-ms 30000}

(llm/generate ai
              {:retry {:max-attempts 5 :base-delay-ms 1000 :max-delay-ms 60000}}
              "long-running task")

;; Disable retries entirely:
(llm/generate ai {:retry {:max-attempts 1}} "no-retry call")
```

Backoff is exponential, capped at `:max-delay-ms`.

## ClojureScript / Node

Same source tree, same API surface, async entry points:

```clojure
(require '[co.poyo.clj-llm.core :as llm]
         '[co.poyo.clj-llm.backend.openai :as openai])

(def ai (openai/backend
          {:api-key (.. js/process -env -OPENAI_API_KEY)
           :defaults {:model "gpt-4o-mini"}}))

;; Promise (most JS-friendly)
(-> (llm/generate-promise ai "hello")
    (.then (fn [r] (println (:text r))))
    (.catch (fn [e] (println "Error:" (.-message e)))))

;; core.async channel (cross-platform, also works on JVM)
(require '[clojure.core.async :as a])
(a/go
  (let [r (a/<! (llm/generate-ch ai "hello"))]
    (if (instance? js/Error r)
      (println "Error:" (.-message r))
      (println (:text r)))))
```

The CLJS HTTP layer uses native `js/fetch` (Node 18+, no polyfill needed). The Vertex backend is JVM-only — token refresh from inside a sandboxed Node runtime is a host-RPC concern outside this library.

## Babashka

Works out of the box — the HTTP layer switches automatically between `java.net.http` and `babashka.http-client`.

```bash
#!/usr/bin/env bb
(require '[co.poyo.clj-llm.core :as llm]
         '[co.poyo.clj-llm.backend.openai :as openai])

(def ai (openai/backend {:api-key "sk-..."
                         :defaults {:model "gpt-4o-mini"}}))
(println (:text (llm/generate ai "Hello from Babashka!")))
```
