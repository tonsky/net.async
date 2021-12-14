(ns net.async.tcp
  (:require
    [clojure.set :as set]
    [clojure.string :as string]
    [clojure.tools.logging :as logging]
    [clojure.core.async :refer [>!! <!! chan close! go <! >! thread timeout alt!]])
  (:import
    [java.util UUID]
    [java.nio  ByteBuffer]
    [java.io   IOException]
    [java.net  InetSocketAddress StandardSocketOptions]
    [java.nio.channels Selector SelectionKey ServerSocketChannel SocketChannel SelectableChannel ClosedChannelException]))

;; UTILITY

(defn ^InetSocketAddress inet-addr [{:keys [^String host ^Integer port]}]
  (if host
    (InetSocketAddress. host port)
    (InetSocketAddress. port)))

(defn buf ^ByteBuffer [size]
  (ByteBuffer/allocate size))

(defn buf-array [& xs]
  (into-array ByteBuffer xs))

(defn in-thread [^String name f & args]
  (doto
    (Thread. #(try
                (apply f args)
                (catch Throwable e
                  (logging/error e "thread died")))
             name)
    (.start)))

(defn now [] (System/currentTimeMillis))

;; SOCKET CALLBACKS

(defn on-read-ready [socket-ref]
  (let [{:keys [read-bufs read-chan]} @socket-ref
        [^ByteBuffer head-buf ^ByteBuffer body-buf] read-bufs]
    (case (count read-bufs)
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
      (buf-array (doto (buf 4) (.putInt 0 (count payload)))
                 (ByteBuffer/wrap payload)))))

(defn on-write-done [socket-ref]
  (swap! socket-ref dissoc :write-bufs)
  (go
    (alt!
      (:write-chan @socket-ref)
        ([payload]
          (if payload
            (swap! socket-ref assoc :write-bufs (write-bufs payload))
            (swap! socket-ref assoc :state :closed)))
      (timeout (:heartbeat-period @socket-ref))
        ([_] (swap! socket-ref assoc :write-bufs (write-bufs []))))))

(defn new-socket [opts]
  (doto
    (atom (merge { :id         (str "/" (+ 1000 (rand-int 8999)))
                   :read-chan  (chan)
                   :write-chan (chan)
                   :reconnect-period 1000
                   :heartbeat-period 5000
                   :heartbeat-timeout (* 4 (:heartbeat-period opts 5000)) }
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
  (let [opts (merge
               { :net-chan net-chan
                 :read-chan  ((:read-chan-fn @socket-ref))
                 :write-chan ((:write-chan-fn @socket-ref)) }
               (select-keys @socket-ref [:heartbeat-period :heartbeat-timeout]))
        new-socket-ref (new-socket opts)]
    (logging/debug "Accepted new socket" (:id @new-socket-ref))
    (on-connected new-socket-ref)
    (go
      (>! (:accept-chan @socket-ref) (client-socket new-socket-ref))
      (swap! socket-ref assoc :state :accepting))
    new-socket-ref))

(defn on-conn-dropped [socket-ref]
  (let [{:keys [state addr read-chan write-chan write-bufs reconnect-period]} @socket-ref]
    (swap! socket-ref dissoc :read-bufs)
    (doseq [^ByteBuffer buf write-bufs] (.rewind buf))
    (if addr
      (do
        (swap! socket-ref assoc :state :disconnected)
        (go
          (>! read-chan :disconnected)
          (<! (timeout reconnect-period))
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

(defn add-watches [{:keys [sockets ^Selector selector running?]}]
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

(defn exhausted? [bufs]
  (zero? (.remaining ^ByteBuffer (last bufs))))

(defn close-net-chan
  ([socket-ref]
   (close-net-chan nil socket-ref))
  ([selector socket-ref]
   (try
     (when-let [^SelectableChannel net-chan (:net-chan @socket-ref)]
       (.close net-chan))
     (when selector
       (.close selector))
     (catch IOException _ :ignore))
   (swap! socket-ref dissoc :net-chan)))

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
          :let [{:keys [state last-read id heartbeat-timeout]} @socket-ref]]
    (when (and last-read
               (= state :connected)
               (< (+ @last-read heartbeat-timeout) (now)))
      (logging/debug "Socket stuck" id)
      (close-net-chan socket-ref)
      (on-conn-dropped socket-ref))))

(defn event-loop-impl [{:keys [sockets started? running? ^Selector selector] :as env}]
  (add-watches env)
  (deliver started? true)
  (loop []
    (detect-connecting sockets)
    (detect-stuck sockets)
    (detect-dead sockets)
    (doseq [socket-ref @sockets
            :let [{:keys [^SelectableChannel net-chan state] :as socket} @socket-ref]]
      (let [opts (select-opts socket)]
        (when (and (not= 0 opts) net-chan)
          (.register net-chan selector opts socket-ref))))
    (.select selector 1000)
    (doseq [^SelectionKey key (.selectedKeys selector)
            :let [socket-ref (.attachment key)
                  {:keys [net-chan read-bufs write-bufs last-read id]} @socket-ref]]
      (try
        (when (and (.isReadable key) net-chan read-bufs)
          (let [read (.read ^SocketChannel net-chan read-bufs 0 (count read-bufs))]
            (reset! last-read (now))
            (if (= -1 read)
              (throw (ClosedChannelException.))
              (when (exhausted? read-bufs)
                (on-read-ready socket-ref)))))

        (when (and (.isWritable key) net-chan write-bufs)
          (.write ^SocketChannel net-chan write-bufs 0 (count write-bufs))
          (when (exhausted? write-bufs)
            (on-write-done socket-ref)))

        (when (and (.isConnectable key) net-chan)
          (.finishConnect ^SocketChannel net-chan)
          (logging/debug "Connected" id)
          (on-connected socket-ref))

        (when (.isAcceptable key)
          (let [new-net-chan (.accept ^ServerSocketChannel net-chan)]
            (.configureBlocking new-net-chan false)
            (swap! sockets conj (on-accepted socket-ref new-net-chan))))

        (catch Exception e
          (cond
            (instance? java.net.ConnectException e)
              (logging/debug "Cannot connect" id (.getMessage e))
            (instance? IOException e)
              (logging/debug "Socket closed" id)
            :else
              (logging/error e "Socket error" id))
          (close-net-chan socket-ref)
          (on-conn-dropped socket-ref))))

    (.clear (.selectedKeys selector))
    (when @running?
      (recur)))
    (doseq [socket-ref @sockets]
      (close-net-chan selector socket-ref)))

(defn event-loop []
  (let [selector (Selector/open)
        env      { :selector selector
                   :started? (promise)
                   :running? (atom true)
                   :sockets  (atom #{}) }
        thread   (in-thread "net.async.tcp/event-loop" event-loop-impl env)]
    @(:started? env)
    (assoc env :thread thread)))

(defn shutdown! [event-loop]
  (reset! (:running? event-loop) false))

;; CLIENT INTERFACE

(defn connect
  "env                  :: result of (event-loop)
  to                    :: map of {:host <String> :port <int>}
  Possible opts are:
   - :read-chan         :: <chan> to populate with reads from socket, defaults to (chan)
   - :write-chan        :: <chan> to schedule writes to socket, defaults to (chan)
   - :heartbeat-period  :: Heartbeat interval, ms. Defaults to 5000
   - :heartbeat-timeout :: When to consider connection stale. Default value is (* 4 heartbeat-period)"
  [env to & {:as opts}]
  (let [addr (inet-addr to)
        socket-ref (new-socket (merge opts
                                 {:addr  addr
                                  :state :connecting}))]
    (swap! (:sockets env) conj socket-ref)
    (client-socket socket-ref)))

(defn accept
  "env                  :: result of (event-loop)
  at                    :: map of {:host <String> :port <int>}, :host defaults to wildcard address
  Possible opts are:
   - :accept-chan       :: <chan> to populate with accepted client sockets, defaults to (chan)
  Following options will be translated to client sockets created by accept:
   - :read-chan-fn      :: no-args fn returning <chan>, will be used to create :read-chan for accepted sockets
   - :write-chan-fn     :: no-args fn returning <chan>, will be used to create :write-chan for accepted sockets
   - :heartbeat-period  :: Heartbeat interval, ms. Default value is 5000
   - :heartbeat-timeout :: When to consider connection stale. Default value is (* 4 heartbeat-period)"
  [env at & {:as opts}]
  (let [addr (inet-addr at)
        net-chan   (doto (ServerSocketChannel/open)
                         (.setOption StandardSocketOptions/SO_REUSEADDR true)
                         (.bind addr)
                         (.configureBlocking false))
        socket-ref (atom (merge
                           { :net-chan      net-chan
                             :state         :accepting
                             :accept-chan   (chan)
                             :read-chan-fn  #(chan)
                             :write-chan-fn #(chan) }
                           opts))]
    (swap! (:sockets env) conj socket-ref)
    { :accept-chan (:accept-chan @socket-ref) }))
