(ns clio.domain.store-test
  (:require [clio.domain.event-hash :as event-hash]
            [clio.domain.ledger-text :as ledger-text]
            [clio.domain.store :as store]
            [clio.law.ledger :as ledger]
            [clio.store-samples :as samples]
            #?(:clj [clio.extern.jvm.crypto :as crypto]
               :cljs [clio.extern.js.crypto :as crypto])
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer [deftest is]])))

(defn- sha [text] (crypto/sha256 text))

(defn- replay
  [events]
  (reduce (fn [acc event]
            (:events (store/apply-admit acc event sha)))
          []
          events))

(defn- error [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) cause
         (:clio/error (ex-data cause)))))

(deftest replay-preserves-hash-order-and-causes
  (doseq [events [(samples/rich-events sha)
                  (samples/plain-events sha)
                  (samples/gapped-events)]]
    (let [stored (replay (reverse events))]
      (samples/assert-preserved events stored sha)
      (is (= (store/head-hash events sha) (store/head-hash stored sha))))))

(deftest transfer-composes-on-the-event-vector
  (let [events (samples/rich-events sha)
        once (replay events)
        via (replay (replay once))
        direct (replay once)]
    (is (= (store/export-events via) (store/export-events direct)))
    (is (= (store/head-hash via sha) (store/head-hash direct sha)))))

(deftest an-exact-retry-does-not-grow-the-vector
  (let [events (samples/plain-events sha)
        once (replay events)
        again (reduce (fn [acc event]
                        (let [result (store/apply-admit acc event sha)]
                          (is (= :already-present (:admission/verdict result)))
                          (:events result)))
                      once
                      events)]
    (is (= once again))))

(deftest admission-matches-the-ledger-law
  (let [events (samples/plain-events sha)
        stored (replay events)
        twin (first events)
        other-id (assoc twin :event/id (samples/event-id 14))
        other-slot (assoc twin :event/seq 9 :event/id (samples/event-id 15))
        wrong-hash (assoc twin :event/hash (apply str (repeat 64 "c")))]
    (is (= (ledger/append-admission stored twin)
           (store/admit stored twin sha)))
    (is (= :id-collision (:admission/verdict (store/admit stored (assoc twin :event/data {:n 9}) sha))))
    (is (= :stream-slot-conflict (:admission/verdict (store/admit stored other-id sha))))
    (is (= :appendable (:admission/verdict (store/admit stored other-slot sha))))
    (is (= :rejected (:admission/verdict (store/admit stored wrong-hash sha))))
    (is (= :clio.ledger/event-hash-mismatch
           (:admission/reason (store/admit stored wrong-hash sha))))
    (is (= stored (:events (store/apply-admit stored wrong-hash sha))))
    (is (= :clio.ledger/invalid-identity
           (:admission/reason (store/admit stored (assoc twin :event/causes [(:event/id twin)]) sha))))))

(deftest read-range-is-inclusive-and-ordered
  (let [events (replay (samples/rich-events sha))]
    (is (= [1 2] (mapv :event/seq (store/read-range events "post:p1" {}))))
    (is (= [2] (mapv :event/seq (store/read-range events "post:p1" {:from 2 :to 2}))))
    (is (= [] (store/read-range events "post:p1" {:from 4 :to 2})))
    (is (= :clio.ledger/invalid-range (error #(store/read-range events "post:p1" {:from 0}))))))

(deftest edn-and-jsonl-text-round-trip-under-a-short-printer
  (binding [*print-length* 1]
    (doseq [events [(samples/rich-events sha) (samples/gapped-events)]
            :let [ordered (store/export-events events)]]
      (is (= ordered (ledger-text/parse-edn (ledger-text/emit-edn ordered))))
      (is (= ordered (ledger-text/parse-jsonl (ledger-text/emit-jsonl ordered)))))))

(deftest empty-ledgers-share-one-head
  (let [empty-hash (store/head-hash [] sha)]
    (is (re-matches #"[0-9a-f]{64}" empty-hash))
    (is (= empty-hash (store/head-hash [] sha)))
    (is (not= empty-hash (store/head-hash (samples/gapped-events) sha)))))

(deftest sealing-does-not-change-a-later-hash-check
  (let [event (first (samples/gapped-events))
        sealed (event-hash/seal event sha)]
    (is (= :appendable (:admission/verdict (store/admit [] sealed sha))))
    (is (= :appendable (:admission/verdict (store/admit [] event sha))))))
