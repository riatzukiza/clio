(ns clio.infra.store.line-file
  "Append-only line files.

   EDN lines match `clio.infra.ledger`'s `pr-str` records. JSONL lines are
   one tagged JSON document each. The inode lock is the one the file ledger
   uses, so the two writers exclude each other on an EDN file. Schema
   validation stays in `append-event!`; this adapter admits by the ledger law."
  (:require [clio.domain.ledger-text :as ledger-text]
            [clio.domain.store :as domain]
            [clio.infra.store :as store]
            [clio.shape.json :as json]
            #?(:clj [clio.extern.jvm.crypto :as crypto]
               :cljs [clio.extern.js.crypto :as crypto])
            #?(:clj [clio.extern.jvm.fs :as fs]
               :cljs [clio.extern.js.fs :as fs])
            [clojure.string :as str]))

(defn- sha256 [text]
  (crypto/sha256 text))

(defn- require-file!
  [path]
  (when-not (fs/exists? path)
    (throw (ex-info "Ledger file does not exist"
                    {:clio/error :clio.ledger/missing-file
                     :path path}))))

(defn- parse [codec text path]
  (try
    (case codec
      :edn (ledger-text/parse-edn text)
      :jsonl (ledger-text/parse-jsonl text))
    (catch #?(:clj Exception :cljs :default) cause
      (throw (ex-info (ex-message cause)
                      (assoc (or (ex-data cause) {}) :path path)
                      cause)))))

(defn- encode-line [codec event]
  (str (case codec
         :edn (ledger-text/pr-event event)
         :jsonl (json/to-json event))
       "\n"))

(defn- read-events [path codec]
  (require-file! path)
  (let [lock (fs/acquire-read-lock! path)]
    (try
      (parse codec (fs/read-locked-text lock) path)
      (finally
        (fs/release-lock! lock)))))

(defrecord LineStore [path codec]
  store/LedgerStore
  (append [store event]
    (require-file! path)
    (let [lock (fs/acquire-lock! path)]
      (try
        (let [text (fs/read-locked-text lock)
              events (parse codec text path)
              admission (domain/admit events event sha256)
              admission (if (= :appendable (:admission/verdict admission))
                          (do
                            (let [delimiter (if (or (str/blank? text)
                                                    (str/ends-with? text "\n"))
                                              ""
                                              "\n")]
                              (fs/append-locked-text! lock
                                                      (str delimiter (encode-line codec event))))
                            (assoc admission :admission/verdict :appended))
                          admission)]
          (store/verdict admission store))
        (finally
          (fs/release-lock! lock)))))
  (read-range [_ stream opts]
    (domain/read-range (read-events path codec) stream opts))
  (streams [_]
    (domain/streams (read-events path codec)))
  (head-hash [_]
    (domain/head-hash (read-events path codec) sha256))
  (release [store] store))

(defn open-edn [path]
  (require-file! path)
  (->LineStore path :edn))

(defn create-edn [path]
  (fs/create-exclusive! path)
  (->LineStore path :edn))

(defn open-jsonl [path]
  (require-file! path)
  (->LineStore path :jsonl))

(defn create-jsonl [path]
  (fs/create-exclusive! path)
  (->LineStore path :jsonl))
