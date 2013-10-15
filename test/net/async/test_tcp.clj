(ns net.async.test-tcp
  (:require
    [clojure.tools.logging :as logging]
    [clojure.core.async :refer [>!! <!! chan close! go <! >! thread timeout alts!!]])
  (:use
    clojure.test
    net.async.tcp))

(defn write-str [socket s]
  (>!! (:write-chan socket) (.getBytes s)))

(defn read-str [socket]
  (when-let [msg (<!! (:read-chan socket))]
    (cond
      (keyword? msg) msg
      :else          (String. msg))))

(deftest test-push
  (let [host1    (event-loop)
        host2    (event-loop)
        accepter (accept host1 {:port 8881})
        client   (connect host2 {:host "localhost" :port 8881})
        server   (<!! (:accept-chan accepter))]
    (write-str client "abc")
    (write-str client "def")
    (write-str client "xyz")
    (close! (:write-chan client))
    (is (= (repeatedly 5 #(read-str server)) [:connected "abc" "def" "xyz" :closed]))
    (reset! (:running? host1) false)
    (reset! (:running? host2) false)))

(deftest test-poll
  (let [host1    (event-loop)
        host2    (event-loop)
        acceptor (accept host1 {:port 8882})
        client   (connect host2 {:host "localhost" :port 8882})
        server   (<!! (:accept-chan acceptor))]
    (write-str server "abc")
    (write-str server "def")
    (write-str server "xyz")
    (is (= (repeatedly 4 #(read-str client)) [:connected "abc" "def" "xyz"]))
    (close! (:write-chan client))
    (reset! (:running? host1) false)
    (reset! (:running? host2) false)))

(deftest test-request-reply
  (let [host1    (event-loop)
        host2    (event-loop)
        acceptor (accept host1 {:port 8883})
        client   (connect host2 {:host "localhost" :port 8883})
        server   (<!! (:accept-chan acceptor))]
    (is (= (read-str server) :connected))
    (is (= (read-str client) :connected))
    (write-str client "req")
    (is (= (read-str server) "req"))
    (write-str server "rep")
    (is (= (read-str client) "rep"))
    (close! (:write-chan client))
    (reset! (:running? host1) false)
    (reset! (:running? host2) false)))

(deftest test-two-clients
  (let [host1    (event-loop)
        host2    (event-loop)
        host3    (event-loop)
        acceptor (accept host1 {:port 8884})
        client1  (connect host2 {:host "localhost" :port 8884})
        server1  (<!! (:accept-chan acceptor))
        client2  (connect host3 {:host "localhost" :port 8884})
        server2  (<!! (:accept-chan acceptor))]
    (is (= (read-str server1) :connected))
    (is (= (read-str server2) :connected))
    (is (= (read-str client1) :connected))
    (is (= (read-str client2) :connected))

    (write-str client1 "req1")
    (write-str client2 "req2")
    (is (= (read-str server2) "req2"))
    (is (= (read-str server1) "req1"))
    (write-str server1 "rep1")
    (write-str server2 "rep2")
    (is (= (read-str client2) "rep2"))
    (is (= (read-str client1) "rep1"))

    (close! (:write-chan client1))
    (close! (:write-chan client2))

    (is (= (read-str server2) :closed))
    (is (= (read-str server1) :closed))

    (reset! (:running? host1) false)
    (reset! (:running? host2) false)
    (reset! (:running? host3) false)))

(deftest test-auto-reconnect
  (let [host1    (event-loop)
        host2    (event-loop)
        acceptor (accept host1 {:port 8885})
        client   (connect host2 {:host "localhost" :port 8885})
        server   (<!! (:accept-chan acceptor))]
    (is (= (read-str server) :connected))
    (is (= (read-str client) :connected))
    (write-str client "req")
    (is (= (read-str server) "req"))

    (reset! (:running? host1) false)
    (is (= (read-str client) :disconnected))

    (let [host1    (event-loop)
          acceptor (accept host1 {:port 8885})
          server   (<!! (:accept-chan acceptor))]
      (is (= (read-str client) :connected))
      (is (= (read-str server) :connected))
      (write-str client "req")
      (is (= (read-str server) "req"))

      (reset! (:running? host1) false)
      (reset! (:running? host2) false))))

(deftest test-close-during-disconnected
  (let [host1    (event-loop)
        host2    (event-loop)
        acceptor (accept host1 {:port 8885})
        client   (connect host2 {:host "localhost" :port 8885})
        server   (<!! (:accept-chan acceptor))]
    (is (= (read-str server) :connected))
    (is (= (read-str client) :connected))
    (write-str client "req")
    (is (= (read-str server) "req"))

    (reset! (:running? host1) false)
    (is (= (read-str client) :disconnected))

    (close! (:write-chan client))
    (is (= (read-str client) :closed))

    (reset! (:running? host1) false)
    (reset! (:running? host2) false)))

;; SAMPLE ECHO SERVER
;; lein trampoline run -m synchro.test-tcp/test-server &
;; lein trampoline run -m synchro.test-tcp/test-client

(defn parse-endpoint [endpoint]
  (let [[_ host port] (re-matches #"(?:([^:]*)\:)?(\d+)" endpoint)]
    {:host host
     :port (Long/parseLong port)}))

(defn test-server [& [endpoint]]
  (let [addr     (parse-endpoint (or endpoint "127.0.0.1:8123"))
        acceptor (accept (event-loop) addr)]
    (loop []
      (logging/info "ACCEPTING")
      (when-let [server (<!! (:accept-chan acceptor))]
        (go
          (loop []
            (when-let [msg (<! (:read-chan server))]
              (if (keyword? msg)
                (logging/info "status" msg)
                (let [msg (String. msg)]
                  (logging/info "RECEIVED" msg)
                  (<! (timeout (rand-int 2000)))
                  (>! (:write-chan server) (.getBytes (str "ECHO/" msg)))))
              (recur)))
          (logging/info "Server thread finished"))
        (recur)))
    (logging/info "Acceptor thread finished")))

(defn test-client [& [endpoint]]
  (let [addr   (parse-endpoint (or endpoint "127.0.0.1:8123"))
        client (connect (event-loop) addr)
        id     (str (+ 100 (rand-int 899)))]
    (loop []
      (let [msg (str id "-" (rand-int 100000))]
        (go (>! (:write-chan client) (.getBytes msg)))
        (loop []
          (let [read (<!! (:read-chan client))]
            (logging/info "READ" (if (keyword? read) read (String. read)))
            (when (and (keyword? read)
                       (not= :connected read))
              (recur))))
        (Thread/sleep (rand-int 3000))
        (recur)))
    (logging/info "Cleint thread finished")))
