(ns clio.extern.jvm.mongo
  "Sync MongoDB driver behind the ledger adapter.

   Documents are `{_id, stream, seq, event}`. `event` is the tagged JSON
   text of the whole event; `seq` is an int64 index and is refused when it
   does not fit, never narrowed. Connection failures become
   `:clio.mongo/unavailable`. Callers see Clojure data only."
  (:require [clojure.string :as str])
  (:import [com.mongodb MongoException MongoWriteException]
           [com.mongodb.client MongoClients]
           [com.mongodb.client.model Filters IndexOptions Indexes Sorts]
           [java.util ArrayList]
           [org.bson Document]))

(defn configured-uri
  []
  (let [uri (System/getenv "CLIO_MONGO_URI")]
    (when-not (str/blank? uri) uri)))

(defn- with-timeout
  [uri]
  (if (re-find #"serverSelectionTimeoutMS=" uri)
    uri
    (str uri
         (if (str/includes? uri "?") "&" "?")
         "serverSelectionTimeoutMS=1500")))

(defn- unavailable!
  [cause]
  (throw (ex-info "MongoDB is unavailable"
                  {:clio/error :clio.mongo/unavailable}
                  cause)))

(defn- require-open!
  [handle]
  (when-not @(:open? handle)
    (throw (ex-info "MongoDB store is closed"
                    {:clio/error :clio.mongo/closed}))))

(defn- long-seq
  [text]
  (let [n (parse-long text)]
    (when-not (and n (pos? n))
      (throw (ex-info "MongoDB stores :event/seq as a positive int64"
                      {:clio/error :clio.mongo/seq-out-of-range
                       :event/seq text})))
    (long n)))

(defn- both
  "Filters/and is varargs, so two filters go in as one iterable."
  [a b]
  (Filters/and (doto (ArrayList.)
                 (.add a)
                 (.add b))))

(defn- into-vec
  [iterable]
  (let [out (ArrayList.)]
    (.into iterable out)
    (vec out)))

(defn- event-text
  [doc]
  (when doc
    (let [text (.get ^Document doc "event")]
      (when-not (string? text)
        (throw (ex-info "MongoDB event field is not tagged JSON text"
                        {:clio/error :clio.mongo/corrupt})))
      text)))

(defn connect
  [{:keys [uri database collection]}]
  (let [uri (with-timeout uri)
        client (try
                 (MongoClients/create ^String uri)
                 (catch MongoException cause
                   (unavailable! cause)))
        coll (-> client
                 (.getDatabase database)
                 (.getCollection collection))]
    (try
      (.createIndex coll
                    (Indexes/ascending (into-array String ["stream" "seq"]))
                    (doto (IndexOptions.)
                      (.unique true)
                      (.name "clio_stream_seq")))
      {:client client :collection coll :open? (atom true)}
      (catch MongoException cause
        (.close client)
        (unavailable! cause))
      (catch Throwable cause
        (.close client)
        (throw cause)))))

(defn close!
  [{:keys [client collection open?]} drop?]
  (when (compare-and-set! open? true false)
    (try
      (when drop? (.drop collection))
      (finally
        (.close client)))))

(defn insert!
  [handle {:keys [id stream seq event]}]
  (require-open! handle)
  (let [doc (doto (Document.)
              (.append "_id" id)
              (.append "stream" stream)
              (.append "seq" (long-seq seq))
              (.append "event" event))]
    (try
      (.insertOne (:collection handle) doc)
      :inserted
      (catch MongoWriteException cause
        (if (= 11000 (.getCode (.getError cause)))
          (throw (ex-info "MongoDB already has this id or stream slot"
                          {:clio/error :clio.mongo/duplicate}
                          cause))
          (throw cause))))))

(defn find-event
  [handle id]
  (require-open! handle)
  (event-text (.first (.find (:collection handle) (Filters/eq "_id" id)))))

(defn find-slot
  [handle stream seq]
  (require-open! handle)
  (event-text
   (.first (.find (:collection handle)
                  (both (Filters/eq "stream" stream)
                        (Filters/eq "seq" (long-seq seq)))))))

(defn- range-filter
  [stream from to]
  (let [lower (Filters/gte "seq" (long-seq from))
        bounds (if (some? to)
                 (both lower (Filters/lte "seq" (long-seq to)))
                 lower)]
    (both (Filters/eq "stream" stream) bounds)))

(defn find-range
  [handle stream from to]
  (require-open! handle)
  (->> (.sort (.find (:collection handle) (range-filter stream from to))
              (Sorts/ascending (into-array String ["seq"])))
       into-vec
       (mapv event-text)))

(defn find-all
  [handle]
  (require-open! handle)
  (mapv event-text (into-vec (.find (:collection handle)))))

(defn stream-names
  [handle]
  (require-open! handle)
  (mapv str (into-vec (.distinct (:collection handle) "stream" String))))
