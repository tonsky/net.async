(ns net.async.util
  (:require
    [clojure.string :as string]
    [clojure.tools.logging :as logging]
    [clojure.core.async :refer [alts!!]]))

(defn parse-endpoint [endpoint]
  (let [[_ host port] (re-matches #"(?:([^:]*)\:)?(\d+)" endpoint)]
    {:host host
     :port (Long/parseLong port)}))

(defn in-red     [& s] (str "\u001b[1;31m" (string/join " " s) "\u001b[m"))
(defn in-green   [& s] (str "\u001b[1;32m" (string/join " " s) "\u001b[m"))
(defn in-blue    [& s] (str "\u001b[1;34m" (string/join " " s) "\u001b[m"))
(defn in-magenta [& s] (str "\u001b[1;35m" (string/join " " s) "\u001b[m"))
(defn in-cyan    [& s] (str "\u001b[1;36m" (string/join " " s) "\u001b[m"))
(defn in-grey    [& s] (str "\u001b[1;30m" (string/join " " s) "\u001b[m"))

(defmacro spy [prefix body]
 `(let [s# ~body]
    (logging/debug (in-grey ~prefix s#))
    s#))

(defn try-put!!
  "Put `value` to `chan` immediately if possible and return `true`,
   else do nothing and return `false`."
  [chan value]
  (= chan (second (alts!! [[chan value]] :default nil))))

(defn wait-ref
  "Block current thread until `ref` value will satisfy `pred` or `timeout` ms expires.
   Return `true` if exited by satisfying `pred`, `false` if exited by timeout.
   Possible forms:
     (wait-ref ref)              - Wait indefinitely for ref to become not-nil
     (wait-ref ref timeout)      - Wait `timeout` ms for ref to become not-nil
     (wait-ref ref pred)         - Wait indefinitely for ref to satisfy `pred`
     (wait-ref ref pred timeout) - Wait `timeout` ms for ref to satisfy `pred`"
  ([ref]   (wait-ref ref #(not (nil? %)) nil))
  ([ref x] (if (number? x)
             (wait-ref ref #(not (nil? %)) x)
             (wait-ref ref x nil)))
  ([ref pred timeout]
    (let [id        (java.util.UUID/randomUUID)
          satisfied (promise)]
      (add-watch ref id (fn [_ _ _ new] (when (pred new) (deliver satisfied true))))
      (try
        (if timeout
          (deref satisfied timeout false)
          (deref satisfied))
        (finally
          (remove-watch ref id))))))
