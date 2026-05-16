(ns co.poyo.clj-llm.net
  (:require
   #?(:bb   [co.poyo.clj-llm.impl.net.bb :as impl]
      :clj  [co.poyo.clj-llm.impl.net.jvm :as impl]
      :cljs [co.poyo.clj-llm.impl.net.cljs :as impl])))

(defn post-stream
  "Blocking POST (Clojure) / async POST (ClojureScript).

   On Clojure/Babashka returns {:status int :body InputStream} synchronously.
   Throws on connection errors.

   On ClojureScript returns a core.async channel that yields one map of the
   shape {:status int :line-ch <core.async chan of strings>} (where each
   value on :line-ch is a single SSE line, and the channel is closed when
   the stream ends). Connection errors and non-200 responses are placed on
   the returned channel as Throwable/ExceptionInfo values."
  [url headers body]
  (impl/post-stream url headers body))
