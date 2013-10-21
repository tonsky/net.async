(ns net.async.synchro
  (:require
    [clojure.edn :as edn]
    [clojure.core.async :refer [>!! <!! <! >! close! alts!! map> map< go-loop]]
    [net.async.tcp :as tcp]
    [net.async.util :as util]))

;; ====== UTILS ======

(defn to-bytes ^bytes [msg] (.getBytes (pr-str msg)))
(defn from-bytes [^bytes msg] (edn/read-string (String. msg)))
(defn wrap-client [client]
  (-> client
    (update-in [:read-chan]  (fn [ch] (map< #(util/spy "<<<" (if (keyword? %) % (from-bytes %))) ch)))
    (update-in [:write-chan] (fn [ch] (map> #(to-bytes (util/spy ">>>" %)) ch)))))

;; ====== STATE PROTOCOL ======

(defprotocol IState
  (snapshot [_])
  (version  [_])
  (apply-delta    [_ delta])
  (command->delta [_ command]))

;; ====== MASTER ======

(defn send-snapshot! [state client]
  (let [msg { :type     :snapshot
              :version  (version state)
              :snapshot state }]
    (or (util/try-put!! (:write-chan client) msg)
        (close! (:write-chan client)))))

(defn apply-command! [msg state clients client]
  (try
    (let [delta  (command->delta state (:command msg))
          state' (apply-delta state delta)
          announce  { :type :delta
                      :from (version state)
                      :to   (version state')
                      :command-id (:command-id msg)
                      :delta delta }]
      (doseq [client clients]
        (util/try-put!! (:write-chan client) announce))
      state')
    (catch Exception e
      (let [reply { :type       :reply
                    :command-id (:command-id msg)
                    :error      (or (ex-data e) (str (class e) ": " (.getMessage e)))
                    :from       (version state) }]
        (util/try-put!! (:write-chan client) reply))
      state)))

(defn master [init-state acceptor]
  (let [{accept-chan :accept-chan} acceptor]
    (loop [clients #{}
           state init-state]
      (let [[val chan] (alts!! (concat [accept-chan] (mapv :read-chan clients)))
            client     (first (filter #(= (:read-chan %) chan) clients))]
        (cond
          (= chan accept-chan)       (recur (conj clients (wrap-client val)) state)
          (= val :closed)            (recur (disj clients client) state)
          (= val :connected)         (recur clients state)
          (= (:type val) :snapshot?) (do (send-snapshot! state client)
                                         (recur clients state))
          (= (:type val) :command)   (recur clients (apply-command! val state clients client)))))))

;; ====== FOLLOWER ======

(defn deliver-command [msg responses-cache]
  ;; TODO swipe out old commands
  (let [command-id (:command-id msg)]
    (when-let [promise (get @responses-cache command-id)]
      (deliver promise msg)
      (swap! responses-cache dissoc command-id))))

(defn mirror [client]
  (let [state  (atom nil)
        master (wrap-client client)
        responses-cache (atom {})]
    (go-loop []
      (let [msg (<! (:read-chan master))]
        (cond
          (= msg :connected)        (>! (:write-chan master) { :type :snapshot? })
          (= msg :disconnected)     :nop
          (= (:type msg) :snapshot) (reset! state (:snapshot msg))
          (= (:type msg) :delta)    (when-not (nil? @state)
                                       (if (= (:from msg) (version @state))
                                         (swap! state apply-delta (:delta msg))
                                         (>! (:write-chan master) { :type :snapshot? }))
                                       (deliver-command msg responses-cache))
          (= (:type msg) :reply)    (deliver-command msg responses-cache)))
      (recur))
    { :state  state
      :master master 
      :responses-cache responses-cache }))

(defn send-command [mirror command]
  (let [response   (promise)
        command-id (java.util.UUID/randomUUID)
        payload    { :type       :command
                     :command    command
                     :command-id command-id
                     :timestamp  (System/currentTimeMillis) }]
    (>!! (get-in mirror [:master :write-chan]) payload)
    (swap! (:responses-cache mirror) assoc command-id response)
    response))
