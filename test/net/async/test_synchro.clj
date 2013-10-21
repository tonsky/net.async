(ns net.async.test-synchro
  (:require
    [clojure.set :as set]
    [clojure.string :as string]
    [clojure.core.async :refer [chan]]
    [clojure.tools.logging :as logging]
    [net.async.tcp :as tcp]
    [net.async.util :as util])
  (:use
    clojure.test
    net.async.synchro))

(extend-type clojure.lang.APersistentMap
  IState
  (snapshot [this] this)
  (version  [this] (hash this))
  (apply-delta [this delta]
    (let [{:keys [add retract]} delta]
      (merge (apply dissoc this retract) add)))
  (command->delta [this ops]
    (let [new-this  (reduce
                      (fn [state op]
                        (case (:op op)
                          :assoc  (assoc  state (:key op) (:value op))
                          :dissoc (dissoc state (:key op))
                          :ensure (if (= (get-in state (concat [(:key op)] (:path op)) ::not-present) (:value op))
                                    state
                                    (throw (ex-info "Ensure failed"
                                                    { :expected (:value op)
                                                      :got      (get-in state (concat [(:key op)] (:path op)) ::not-present)
                                                      :key  (:key op)
                                                      :path (:path op) })))
                          :update (update-in state (concat [(:key op)] (:path op)) #(apply (resolve (symbol (:fn op))) % (:args op)))))
                      this ops)]
      {:add     (into {} (filter (fn [[k v]] (not= v (this k ::default))) new-this))
       :retract (set/difference (set (keys this)) (set (keys new-this)))})))

(deftest test-map-state
  (let [s0 {:a 7 :b {:x 8}}]
    (are [cmd res] (= (apply-delta s0 (command->delta s0 cmd)) res)
      [{ :op :assoc, :key :a, :value 9 }] {:a 9, :b {:x 8}}
      [{ :op :assoc, :key :c, :value 9 }] {:a 7, :b {:x 8}, :c 9}
      
      [{ :op :dissoc, :key :a }]          { :b {:x 8}}
      [{ :op :dissoc, :key :c }]          s0

      [{ :op :ensure, :key :a, :value 7}]             s0
      [{ :op :ensure, :key :b, :path [:x], :value 8}] s0

      [{ :op :update, :key :a, :fn 'dec}]                        {:a 6, :b {:x 8}}
      [{ :op :update, :key :b, :path [:x], :fn 'inc}]            {:a 7, :b {:x 9}}
      [{ :op :update, :key :b, :path [:x], :fn '-, :args [1 2]}] {:a 7, :b {:x 5}}

      [{ :op :assoc,  :key :c, :value 9 }
       { :op :ensure, :key :c, :value 9 }
       { :op :update, :key :a, :fn 'dec }
       { :op :ensure, :key :a, :value 6 }
       { :op :update, :key :b, :fn 'assoc, :args [:y 2]}
       { :op :ensure, :key :b, :path [:y] :value 2 }
       { :op :dissoc, :key :c }
       { :op :ensure, :key :c, :value ::not-present }] {:a 6, :b {:x 8 :y 2}})

    (is (thrown? clojure.lang.ExceptionInfo (command->delta s0 [{:op :ensure :key :a :value 2 }])))
    (is (thrown? clojure.lang.ExceptionInfo (command->delta s0 [{:op :ensure :key :d :value nil }])))))



;; ====== Test setup =======

(defn start-master [& [endpoint]]
  (let [addr (util/parse-endpoint (or endpoint "8880"))]
    (logging/info "Listening at" addr)
    (master {} (tcp/accept (tcp/event-loop) addr :read-chan-fn  #(chan 10) :write-chan-fn #(chan 10)))))

(defn start-follower [id & [endpoint]]
  (let [addr   (util/parse-endpoint (or endpoint "8880"))
        _      (logging/info "Connecting to" addr)
        client (tcp/connect (tcp/event-loop) addr :read-chan  (chan 10) :write-chan (chan 10))
        mirror (mirror client)
        state  (:state mirror)]
    (util/wait-ref state)
    (add-watch state :debug-state (fn [_ _ _ new] (logging/debug (util/in-green "New state" new))))
    (loop []
      (let [reply (send-command mirror
                    (concat
                      (map (fn [[k v]] {:op :ensure :key k :value v}) @state)
                      [{:op :assoc :key id :value (inc (get @state id 0))}]))
            reply (deref reply 1000 {:type :not-derefed})]
        (logging/debug (case (:type reply)
                         :delta       (util/in-green reply)
                         :reply       (util/in-magenta reply)
                         :not-derefed (util/in-red reply))))
      (Thread/sleep (rand-int 50))
      (recur))))
