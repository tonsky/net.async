(ns net.async.tcp
  (:require
    [clojure.set :as set]
    [clojure.string :as string]
    [clojure.tools.logging :as logging]
    [clojure.core.async :refer [>!! <!! chan close! go <! >! thread timeout alts!]])
  (:import
    [java.util UUID]
    [java.nio  ByteBuffer]
    [java.io   IOException]
    [java.net  InetSocketAddress StandardSocketOptions]
    [java.nio.channels Selector SelectionKey ServerSocketChannel SocketChannel ClosedChannelException]))

;; UTILITY

(defn inet-addr [{:keys [^String host ^Integer port]}]
  (if host
    (InetSocketAddress. host port)
    (InetSocketAddress. port)))

(defn buf [size]
  (ByteBuffer/allocate size))

(defn buf-array [& xs]
  (into-array ByteBuffer xs))

(def ^:const heartbeat 5000)

(defn in-thread [name f & args]
  (doto
    (Thread. ^Runnable #(try
                (apply f args)
                (catch Throwable e
                  (logging/error e "thread died")))
             name)
    (.start)))

(defn wait-ref [ref pred]
  (let [key   (rand)
        latch (promise)]
    (add-watch ref key (fn [_ _ _ new] (when (pred new) (deliver latch true))))
    (when-not (pred @ref)
      @latch)
    (remove-watch ref key)))

(defn now [] (System/currentTimeMillis))

;; SOCKET CALLBACKS

(defn on-read-ready [socket-ref]
  (let [{:keys [read-bufs read-chan]} @socket-ref
        [head-buf body-buf] read-bufs]
    (case (alength read-bufs)
      1 (let [size (.getInt head-buf 0)]
          (if (zero? size)
            (.rewind head-buf) ;; heartbeat
            (swap! socket-ref assoc :read-bufs (buf-array head-buf (buf size)))))
      2 (do
          (swap! socket-ref dissoc :read-bufs)
          (go
            (>! read-chan (.array body-buf))
            (.rewind head-buf)
            (swap! socket-ref assoc :read-bufs (buf-array head-buf)))))))

(defn write-bufs [payload]
  (when payload
    (if (empty? payload)
      (buf-array (doto (buf 4) (.putInt 0 0))) ;; heartbeat
      (buf-array (doto (buf 4) (.putInt 0 (alength payload)))
                 (ByteBuffer/wrap payload)))))

(defn on-write-done [socket-ref]
  (swap! socket-ref dissoc :write-bufs)
  (go
    (let [[payload chan] (alts! [(:write-chan @socket-ref) (timeout heartbeat)])]
      (if (= chan (:write-chan @socket-ref))
        (if payload
          (swap! socket-ref assoc :write-bufs (write-bufs payload))
          (swap! socket-ref assoc :state :closed))
        (swap! socket-ref assoc :write-bufs (write-bufs []))))))

(defn new-socket [& {:as opts}]
  (doto
    (atom (merge { :id         (str "/" (+ 1000 (rand-int 8999)))
                   :read-chan  (chan)
                   :write-chan (chan) }
                 opts))
    (on-write-done)))

(defn client-socket [socket-ref]
  (select-keys @socket-ref [:read-chan :write-chan]))

(defn on-connected [socket-ref]
  (swap! socket-ref assoc
         :state :connected
         :read-bufs (buf-array (buf 4))
         :last-read (atom (now)))
  (go
    (>! (:read-chan @socket-ref) :connected)))

(defn on-accepted [socket-ref net-chan]
  (swap! socket-ref assoc :state :not-accepting)
  (let [new-socket-ref (new-socket :net-chan net-chan)]
    (logging/debug "Accepted new socket" (:id @new-socket-ref))
    (on-connected new-socket-ref)
    (go
      (>! (:accept-chan @socket-ref) (client-socket new-socket-ref))
      (swap! socket-ref assoc :state :accepting))
    new-socket-ref))

(defn on-conn-dropped [socket-ref]
  (let [{:keys [state addr read-chan write-chan write-bufs]} @socket-ref]
    (swap! socket-ref dissoc :read-bufs)
    (doseq [buf write-bufs] (.rewind buf))
    (if addr
      (do
        (swap! socket-ref assoc :state :disconnected)
        (go
          (>! read-chan :disconnected)
          (<! (timeout 1000))
          (swap! socket-ref assoc
                 :state :connecting)))
      (swap! socket-ref assoc :state :closed))))

(defn on-conn-closed [socket-ref]
  (let [{:keys [read-chan write-chan]} @socket-ref]
    (go
      (>! read-chan :closed)
      (close! read-chan)
      (close! write-chan))))

;; EVENT LOOP

(defn add-watches [{:keys [sockets selector running?]}]
  (add-watch running? :wakeup (fn [_ _ _ _] (.wakeup selector)))
  (add-watch sockets :wakeup
             (fn [_ _ old new]
               (doseq [socket (set/difference old new)] (remove-watch socket :wakeup))
               (doseq [socket (set/difference new old)] (add-watch socket :wakeup (fn [_ _ _ _] (.wakeup selector))))
               (.wakeup selector))))

(defn select-opts [socket]
  (let [state (:state socket)]
    (cond-> 0
      (= :accepting state)  (bit-or SelectionKey/OP_ACCEPT)
      (= :connecting state) (bit-or SelectionKey/OP_CONNECT)
      (and (= :connected state) (:read-bufs socket))  (bit-or SelectionKey/OP_READ)
      (and (= :connected state) (:write-bufs socket)) (bit-or SelectionKey/OP_WRITE))))

(defn opts-str [opts]
  (str
    (if (zero? (bit-and opts SelectionKey/OP_ACCEPT))  "-" "A")
    (if (zero? (bit-and opts SelectionKey/OP_CONNECT)) "-" "C")
    (if (zero? (bit-and opts SelectionKey/OP_READ))    "-" "R")
    (if (zero? (bit-and opts SelectionKey/OP_WRITE))   "-" "W")))

(defn exhausted? [bufs]
  (zero? (.remaining (last bufs))))

(defn close-net-chan [socket-ref]
  (try
    (when-let [net-chan (:net-chan @socket-ref)]
      (.close net-chan))
    (catch IOException e)) ;; ignore
  (swap! socket-ref dissoc :net-chan))

(defn detect-connecting [sockets]
  (doseq [socket-ref @sockets
          :let [{:keys [state net-chan addr id]} @socket-ref]]
    (when (and (= :connecting state)
               (nil? net-chan))
      (logging/debug "Connecting socket" id)
      (let [net-chan (doto (SocketChannel/open)
                           (.configureBlocking false))]
        (swap! socket-ref assoc :net-chan net-chan)
        (.connect net-chan addr)))))

(defn detect-dead [sockets]
  (doseq [socket-ref @sockets
          :let [{:keys [state id]} @socket-ref]]
    (when (= state :closed)
      (logging/debug "Deleting socket" id)
      (swap! sockets disj socket-ref)
      (close-net-chan socket-ref)
      (on-conn-closed socket-ref))))

(defn detect-stuck [sockets]
  (doseq [socket-ref @sockets
          :let [{:keys [state last-read id]} @socket-ref]]
    (when (and last-read
               (= state :connected)
               (< (+ @last-read (* 4 heartbeat)) (now)))
      (logging/debug "Socket stuck" id)
      (close-net-chan socket-ref)
      (on-conn-dropped socket-ref))))

(defn event-loop-impl [{:keys [sockets running? selector] :as env}]
  (add-watches env)
  (reset! running? true)
  (loop []
    (detect-connecting sockets)
    (detect-stuck sockets)
    (detect-dead sockets)
    (doseq [socket-ref @sockets
            :let [{:keys [net-chan state] :as socket} @socket-ref]]
      (let [opts (select-opts socket)]
        (when (and (not= 0 opts) net-chan)
          (.register net-chan selector opts socket-ref))))
    (.select selector 1000)
    (doseq [key (.selectedKeys selector)
            :let [socket-ref (.attachment key)
                  {:keys [net-chan read-bufs write-bufs last-read id]} @socket-ref]]
      (try
        (when (and (.isReadable key) net-chan read-bufs)
          (let [read (.read net-chan read-bufs 0 (alength read-bufs))]
            (reset! last-read (now))
            (if (= -1 read)
              (throw (ClosedChannelException.))
              (when (exhausted? read-bufs)
                (on-read-ready socket-ref)))))

        (when (and (.isWritable key) net-chan write-bufs)
          (.write net-chan write-bufs 0 (alength write-bufs))
          (when (exhausted? write-bufs)
            (on-write-done socket-ref)))

        (when (and (.isConnectable key) net-chan)
          (.finishConnect net-chan)
          (logging/debug "Connected" id)
          (on-connected socket-ref))

        (when (.isAcceptable key)
          (let [new-net-chan (.accept net-chan)]
            (.configureBlocking new-net-chan false)
            (swap! sockets conj (on-accepted socket-ref new-net-chan))))

        (catch java.net.ConnectException e
          (logging/debug "Cannot connect" id (.getMessage e))
          (close-net-chan socket-ref)
          (on-conn-dropped socket-ref))
        (catch ClosedChannelException e
          (logging/debug "Socket closed" id)
          (close-net-chan socket-ref)
          (on-conn-dropped socket-ref))
        (catch IOException e ;; handle IOException: Connection reset by peer
          (logging/warn e "Socket error" id)
          (close-net-chan socket-ref)
          (on-conn-dropped socket-ref))))

    (.clear (.selectedKeys selector))
    (if @running?
      (recur)
      (doseq [socket-ref @sockets]
        (close-net-chan socket-ref)))))

(defn event-loop []
  (let [selector (Selector/open)
        running? (atom false)
        env      { :selector selector
                   :running? running?
                   :sockets  (atom #{}) }
        thread   (in-thread "synchro-event-loop" event-loop-impl env)]
    (wait-ref running? true?)
    (assoc env :thread thread)))

;; CLIENT INTERFACE

(defn connect [env to]
  (let [addr (inet-addr to)
        socket-ref (new-socket :addr  addr
                               :state :connecting)]
    (swap! (:sockets env) conj socket-ref)
    (client-socket socket-ref)))

(defn accept [env at]
  (let [addr (inet-addr at)
        net-chan   (doto (ServerSocketChannel/open)
                         (.setOption StandardSocketOptions/SO_REUSEADDR true)
                         (.bind addr)
                         (.configureBlocking false))
        socket-ref (atom { :net-chan    net-chan
                           :state       :accepting
                           :accept-chan (chan) })]
    (swap! (:sockets env) conj socket-ref)
    { :accept-chan (:accept-chan @socket-ref) }))
