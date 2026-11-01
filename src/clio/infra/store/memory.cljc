(ns clio.infra.store.memory
  "In-memory ledger. `append` returns a new store; the previous value is
   unchanged. This is the reference object of the store protocol: every host
   adapter has to agree with it on export order, admission and head hash."
  (:require [clio.domain.store :as domain]
            [clio.infra.store :as store]
            #?(:clj [clio.extern.jvm.crypto :as crypto]
               :cljs [clio.extern.js.crypto :as crypto])))

(defn- sha256 [text]
  (crypto/sha256 text))

(defrecord MemoryStore [events])

(extend-protocol store/LedgerStore
  MemoryStore
  (append [store event]
    (let [result (domain/apply-admit (:events store) event sha256)
          result (if (= :appendable (:admission/verdict result))
                   (assoc result :admission/verdict :appended)
                   result)]
      (store/verdict result (->MemoryStore (:events result)))))
  (read-range [store stream opts]
    (domain/read-range (:events store) stream opts))
  (streams [store]
    (domain/streams (:events store)))
  (head-hash [store]
    (domain/head-hash (:events store) sha256))
  (release [store] store))

(defn store
  "An empty ledger, or `events` already in hand. Events passed here are not
   re-admitted; use `append` when the admission verdict matters."
  ([] (store []))
  ([events] (->MemoryStore (vec events))))
