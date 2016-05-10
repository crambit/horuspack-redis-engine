(ns core.tslog.config
  "tslog for Redis engine"
  (:require [taoensso.carmine :as car]
            [clojure.string :as cstr]))

(defmacro ^:private wcar* [& body] `(car/wcar {:pool {} :spec {:uri "//localhost:6379"}} ~@body))

;; individual sorted set max size
(def ^:private MAX_EVENTS 1000)

(defn persist-event
  "Persist event to a sorted set stored at key"
  [& key]
  (fn [event]
    (let [key (or key "events")]
      (try
        (wcar* (car/zadd key (:time event) event)
               (car/zremrangebyrank key 0 (* -1 MAX_EVENTS)))
        (catch Exception e
          (prn (str "RedisError -> " e)))))))


;; define granularities constants
(def ^:private granularities {:ss {:ttl 300 :duration 1}
                              :mm {:ttl 21600 :duration 60}
                              :hh {:ttl 604800 :duration 3600}
                              :dd {:ttl 2419200 :duration 86400}})

(defn- get-rounded-time
  "Round timestamp to the precision interval (in seconds)."
  [precision & args]
  (let [time (first args)
        time (or time (quot (System/currentTimeMillis) 1000))]
    (* (quot time precision) precision)))

(defn- record-hit
  "Record a hit for the collection at the specified stats granularity."
  [properties key-timestamp tmp-key event]
  (let [tmp-key-count (cstr/join ":" [tmp-key "count"])
        hit-timestamp (get-rounded-time (:duration properties) (:time event))]
    (try
      (wcar* (car/hincrbyfloat tmp-key hit-timestamp (:metric event))
             (car/expireat tmp-key (+ key-timestamp (* 2 (:ttl properties))))
             (car/hincrby tmp-key-count hit-timestamp 1)
             (car/expireat tmp-key-count (+ key-timestamp (* 2 (:ttl properties)))))
      (catch Exception e
        (prn (str "RedisError -> " e))))))

(defn time-series
  "Record events metric for the collection at the specified stats granularities."
  [& {:keys [coll groupby]}]
  (fn [event]
    (doseq [gran granularities]
      (let [coll (or coll (:service event))
            properties (val gran)
            key-timestamp (get-rounded-time (:ttl properties) (:time event))]
        (cond
          (= groupby "host")
            (record-hit properties key-timestamp (cstr/join ":" [coll (name (key gran)) groupby (:host event) key-timestamp]) event)
          (= groupby "service")
            (record-hit properties key-timestamp (cstr/join ":" [coll (name (key gran)) groupby (:service event) properties key-timestamp]) event)
          :else
            (record-hit properties key-timestamp (cstr/join ":" [coll (name (key gran)) key-timestamp]) event))))))

