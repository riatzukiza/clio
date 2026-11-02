(ns clio.infra.store.mongo
  "MongoDB ledger.

   One document per event. The document's `event` field is the tagged JSON
   text, so the stored bytes are the codec's, not a second translation.
   Admission still reads at most the id hit and the slot hit: those are the
   only two events `append-admission` can name. A duplicate-key error from a
   concurrent writer is re-read and returned as that law's verdict.

   `:event/seq` is indexed as an int64. A sequence `parse-long` cannot name
   is refused. The JSON document keeps the original sequence either way; this
   limit is only the index."
  (:require [clio.domain.store :as domain]
            [clio.infra.store :as store]
            [clio.shape.json :as json]
            #?(:clj [clio.extern.jvm.crypto :as crypto]
               :cljs [clio.extern.js.crypto :as crypto])
            #?(:clj [clio.extern.jvm.mongo :as mongo]
               :cljs [clio.extern.js.mongo :as mongo])
            [clojure.string :as str]))

(defn- sha256 [text]
  (crypto/sha256 text))

(defn configured-uri
  []
  (mongo/configured-uri))

(defn- seq-text
  [value]
  (let [text (str value)
        parsed (parse-long text)]
    (when-not (and parsed (pos? parsed) (= text (str parsed)))
      (throw (ex-info "MongoDB stores :event/seq as a positive int64"
                      {:clio/error :clio.mongo/seq-out-of-range
                       :event/seq value})))
    text))

(defn- decode [text]
  (when (some? text)
    (json/from-json text)))

(defn- conflicts
  [handle event]
  (let [by-id (decode (mongo/find-event handle (:event/id event)))
        by-slot (decode (mongo/find-slot handle
                                          (:event/stream event)
                                          (seq-text (:event/seq event))))]
    (vec (distinct (remove nil? [by-id by-slot])))))

(defn- duplicate? [cause]
  (= :clio.mongo/duplicate (:clio/error (ex-data cause))))

(defrecord MongoStore [handle drop-on-release?]
  store/LedgerStore
  (append [this event]
    (let [precheck (domain/admit [] event sha256)
          admission (if (= :rejected (:admission/verdict precheck))
                      precheck
                      (domain/admit (conflicts handle event) event sha256))]
      (if-not (= :appendable (:admission/verdict admission))
        (store/verdict admission this)
        (try
          (mongo/insert! handle {:id (:event/id event)
                                 :stream (:event/stream event)
                                 :seq (seq-text (:event/seq event))
                                 :event (json/to-json event)})
          (store/verdict (assoc admission :admission/verdict :appended) this)
          (catch #?(:clj Exception :cljs :default) cause
            (if-not (duplicate? cause)
              (throw cause)
              (let [again (domain/admit (conflicts handle event) event sha256)]
                (if (= :appendable (:admission/verdict again))
                  (throw (ex-info "MongoDB rejected an append the ledger law had allowed"
                                  {:clio/error :clio.mongo/duplicate}
                                  cause))
                  (store/verdict again this)))))))))
  (read-range [_ stream opts]
    (domain/read-range [] stream opts)
    (let [from (seq-text (:from opts 1))
          to (when (some? (:to opts)) (seq-text (:to opts)))
          events (mapv json/from-json (mongo/find-range handle stream from to))]
      (domain/read-range events stream opts)))
  (streams [_]
    (vec (sort (mongo/stream-names handle))))
  (head-hash [_]
    (domain/head-hash (mapv json/from-json (mongo/find-all handle)) sha256))
  (release [this]
    (mongo/close! handle drop-on-release?)
    this))

(defn open
  "Open `collection` in `database` (default `clio`) at `uri`.
   `:drop-on-release?` drops that collection from `release`."
  [{:keys [uri database collection] :as opts}]
  (let [database (or database "clio")]
    (when-not (and (string? uri) (not (str/blank? uri))
                   (string? collection) (not (str/blank? collection)))
      (throw (ex-info "MongoDB open requires a uri and a collection"
                      {:clio/error :clio.mongo/invalid-options})))
    (->MongoStore (mongo/connect {:uri uri
                                  :database database
                                  :collection collection})
                  (boolean (:drop-on-release? opts)))))

(defn skippable-open?
  "True only for an optional server or driver that is not there.
   A setup failure (`:clio.mongo/driver`) is not skippable."
  [code]
  (contains? #{:clio.mongo/unavailable :clio.mongo/driver-missing} code))

(defn connect
  "Like `open`, but an absent driver or server is `{:skipped code :message _}`.
   Authentication, index, and other setup failures still throw."
  [opts]
  (try
    {:store (open opts)}
    (catch #?(:clj Exception :cljs :default) cause
      (let [code (:clio/error (ex-data cause))]
        (if (skippable-open? code)
          {:skipped code :message (ex-message cause)}
          (throw cause))))))
