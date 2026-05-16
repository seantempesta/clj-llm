(ns co.poyo.clj-llm.stream
  "HTTP → bounded core.async channel of decoded SSE events.

   Cross-platform. The JVM impl reads the response InputStream line-by-line
   on a thread; the CLJS impl pumps the Fetch ReadableStream line-by-line via
   a Promise chain. Both feed a uniform `parse-sse-data` transducer."
  (:require
   [camel-snake-kebab.core   :as csk]
   [camel-snake-kebab.extras :as cske]
   [clojure.core.async       :as a]
   [clojure.string           :as str]
   [co.poyo.clj-llm.net      :as net]
   #?@(:clj [[cheshire.core   :as json]
             [clojure.java.io :as io]]))
  #?(:clj (:import (java.io InputStream))))

;; ════════════════════════════════════════════════════════════════════
;; SSE parsing
;; ════════════════════════════════════════════════════════════════════

(def ^:private ->kebab-key (memoize csk/->kebab-case-keyword))

#?(:clj
   (defn- parse-json [s]
     (json/parse-string s))
   :cljs
   (defn- parse-json [s]
     (js->clj (js/JSON.parse s))))

(defn parse-sse-data
  "Extract and parse JSON from a 'data: …' SSE line.
   Returns a decoded map, or nil for non-data / blank / [DONE].
   Returns an :error map for unparseable JSON so the stream doesn't silently
   swallow provider errors."
  [line]
  (when (and (string? line) (str/starts-with? line "data:"))
    (let [payload (str/trim (subs line 5))]
      (cond
        (empty? payload) nil
        (= "[DONE]" payload) nil
        :else (try
                (cske/transform-keys ->kebab-key (parse-json payload))
                (catch #?(:clj Exception :cljs :default) _
                  {:type "error"
                   :error {:message (str "Unparseable SSE data: " payload)}}))))))

;; ════════════════════════════════════════════════════════════════════
;; HTTP streaming
;; ════════════════════════════════════════════════════════════════════

#?(:clj
   (defn- check-status!
     "Throws ex-info for non-200 responses, draining the body for the message."
     [{:keys [^InputStream body status]}]
     (when (not= 200 status)
       (let [body-str (try (slurp body) (catch Exception _ nil))]
         (throw (ex-info (cond-> (str "HTTP " status)
                           body-str (str ": " body-str))
                         (cond-> {:status status}
                           body-str (assoc :body body-str))))))))

#?(:clj
   (defn open-event-stream [url headers body]
     (let [{:keys [^InputStream body] :as response} (net/post-stream url headers body)]
       (try
         (check-status! response)
         (let [data-ch (a/chan 256 (keep parse-sse-data))
               out-ch  (a/chan 256)]
           (a/pipe data-ch out-ch false)
           (a/thread
             (try
               (with-open [rdr (io/reader body)]
                 (loop []
                   (when-let [line (.readLine rdr)]
                     (when (a/>!! data-ch line)
                       (recur)))))
               (catch Exception e
                 (when-not (.isInterrupted (Thread/currentThread))
                   (a/>!! out-ch e)))
               (finally
                 (a/close! data-ch)
                 (a/close! out-ch))))
           out-ch)
         (catch Exception e
           (.close body)
           (throw e)))))
   :cljs
   (defn open-event-stream
     "CLJS variant. Returns a core.async channel of decoded SSE events.
      Errors (connection failures, non-200) appear on the channel as
      Throwable/ex-info values; the consumer (`provider-request-events` in
      core) is responsible for surfacing them. The channel closes when the
      upstream Fetch stream ends."
     [url headers body]
     (let [out-ch (a/chan 256 (keep parse-sse-data))]
       (a/go
         (let [conn (a/<! (net/post-stream url headers body))]
           (cond
             (nil? conn)
             (a/close! out-ch)

             ;; Connection-level failure: surface and close.
             (instance? js/Error conn)
             (do (a/>! out-ch conn) (a/close! out-ch))

             :else
             (let [{:keys [line-ch]} conn]
               (loop []
                 (let [line (a/<! line-ch)]
                   (cond
                     (nil? line) (a/close! out-ch)
                     (instance? js/Error line)
                     (do (a/>! out-ch line) (a/close! out-ch))
                     :else (do (a/>! out-ch line) (recur)))))))))
       out-ch)))
