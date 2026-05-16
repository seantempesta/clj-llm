(ns co.poyo.clj-llm.impl.net.cljs
  "ClojureScript / Node HTTP POST → core.async channel of SSE lines.

   Uses the global `fetch` (available in Node 18+ and modern browsers).
   The returned :body is consumed as a ReadableStream via the async iterator
   protocol and decoded as UTF-8 line by line.

   The shape returned from `post-stream` is intentionally different from the
   JVM impl: in CLJS we cannot block on the response, so we return a
   core.async channel that yields exactly one map containing the parsed
   line channel. See `co.poyo.clj-llm.net/post-stream` doc."
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]))

(defn- ^js text-decoder []
  (js/TextDecoder. "utf-8"))

(defn- pump-stream
  "Read the fetch Response.body ReadableStream, decode as UTF-8, split on
   newlines, push each non-empty line onto line-ch. Closes line-ch when
   the upstream ends. On error, puts an ex-info on line-ch then closes."
  [^js body line-ch]
  (let [reader  (.getReader body)
        decoder (text-decoder)
        buf     (volatile! "")]
    (letfn [(loop-step []
              (-> (.read reader)
                  (.then (fn [^js result]
                           (if (.-done result)
                             (do
                               (let [tail @buf]
                                 (when (seq tail)
                                   (a/put! line-ch tail)))
                               (a/close! line-ch))
                             (let [chunk (.decode decoder (.-value result) #js {:stream true})
                                   combined (str @buf chunk)
                                   parts (str/split combined #"\n" -1)
                                   ;; last element may be partial — hold it
                                   complete-lines (butlast parts)
                                   tail (last parts)]
                               (vreset! buf tail)
                               (doseq [line complete-lines]
                                 (a/put! line-ch line))
                               (loop-step)))))
                  (.catch (fn [err]
                            (a/put! line-ch
                                    (ex-info (str "Stream read error: " (.-message err))
                                             {:error err}))
                            (a/close! line-ch)))))]
      (loop-step))))

(defn- read-body-text
  "For non-200 responses: drain the body as text. Returns a Promise<string>."
  [^js response]
  (.text response))

(defn post-stream
  "Returns a core.async channel that yields exactly one value:
     - a map {:status int :line-ch <chan of strings>}  on success
     - an ex-info Throwable                            on connection failure

   The line-ch yields one string per SSE line and is closed when the
   underlying stream completes."
  [url headers body]
  (let [out-ch (a/chan 1)
        opts   #js {:method  "POST"
                    :headers (clj->js headers)
                    :body    body}]
    (-> (js/fetch url opts)
        (.then (fn [^js response]
                 (let [status (.-status response)]
                   (cond
                     (and (>= status 200) (< status 300))
                     (let [line-ch (a/chan 256)]
                       (pump-stream (.-body response) line-ch)
                       (a/put! out-ch {:status status :line-ch line-ch})
                       (a/close! out-ch))

                     :else
                     (-> (read-body-text response)
                         (.then (fn [body-str]
                                  (a/put! out-ch
                                          (ex-info (str "HTTP " status ": " body-str)
                                                   {:status status :body body-str}))
                                  (a/close! out-ch))))))))
        (.catch (fn [err]
                  (a/put! out-ch
                          (ex-info (str "Fetch error: " (.-message err))
                                   {:error err}))
                  (a/close! out-ch))))
    out-ch))
