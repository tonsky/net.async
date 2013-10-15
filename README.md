### Network communications with clojure.core.async interface

This library is aimed at providing reliable, high-level, bidirectional message-oriented communication over TCP/IP.

1. Send/receive messages, not data streams. It's what you need most of the time, anyway.
2. Automatic reconnections when connection is lost.
3. Stuck connection detection via heartbeats.
4. Stable interface: chans stay valid to use no matter what socket/network conditions are. net.async will handle underlying resource/socket management for you.
5. No exceptions: disconnects are normal state of network connection and are exposed explicitly at top-level interface.

#### Types

    <event-loop>    is { : running? <atom [true/false]> }
    <client-socket> is { :read-chan  <chan>
                         :write-chan <chan> }
    <accept-socket> is { :accept-chan <chan> }
    <payload>       is byte[]

#### Preparation

```clojure
(require '[clojure.core.async :refer [<!! >!! close!]])
(use 'net.async.tcp)

(event-loop) → <event-loop>
```

#### Client

```clojure
(connect <event-loop> {:host "127.0.0.1" :port 9988}) → <client-socket>
(<!! read-chan) → :connected | :disconnected | :closed | <payload>
(>!! write-chan <payload>)
(close! write-chan)
```

Client will automatically reconnect if it loses its connection to the server. During that period, you'll get `:disconnected` messages from `read-chan`. Once connection is established, you'll get `:connected` message and then normal communication will resume.

#### Server

```clojure
(accept <event-loop> {:port 9988}) → <accept-socket>
(<!! accept-chan)                  → <client-socket>
(close! accept-chan)
```

#### Shutting down

```clojure
(reset! (:running? <event-loop>) false)
```

#### Sample echo server/client

```clojure
(defn echo-server []
  (let [acceptor (accept (event-loop) {:port 8899})]
    (loop []
      (when-let [server (<!! (:accept-chan acceptor))]
        (go
          (loop []
            (when-let [msg (<! (:read-chan server))]
              (when-not (keyword? msg)
                (>! (:write-chan server) (.getBytes (str "ECHO/" (String. msg)))))
              (recur))))
        (recur)))))

(defn echo-client []
  (let [client (connect (event-loop) {:host "127.0.0.1" :port 8899})]
    (loop []
      (go (>! (:write-chan client) (.getBytes (str (rand-int 100000)))))
      (loop []
        (let [read (<!! (:read-chan client))]
          (when (and (keyword? read)
                     (not= :connected read))
            (recur))))
      (Thread/sleep (rand-int 3000))
      (recur))))
```
