(ns clio.infra.store-test
  (:require [clio.domain.event-hash :as event-hash]
            [clio.domain.store :as domain]
            [clio.infra.ledger :as ledger]
            [clio.infra.store :as store]
            [clio.infra.store.edn-lines :as edn-lines]
            [clio.infra.store.jsonl :as jsonl]
            [clio.infra.store.memory :as memory]
            [clio.store-samples :as samples]
            #?(:clj [clio.extern.jvm.crypto :as crypto]
               :cljs [clio.extern.js.crypto :as crypto])
            #?(:clj [clio.extern.jvm.fs :as fs]
               :cljs [clio.extern.js.fs :as fs])
            #?(:clj [clio.extern.jvm.runtime :as runtime]
               :cljs [clio.extern.js.runtime :as runtime])
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer [deftest is]])))

(defn- sha [text] (crypto/sha256 text))

(defn- temp-dir []
  (let [dir (str "/tmp/clio-store-" (runtime/random-uuid))]
    (fs/ensure-dir! dir)
    dir))

(defn- factories [dir]
  (let [n (atom 0)]
    {:memory (fn [] (memory/store))
     :edn (fn []
            (edn-lines/create! (str dir "/e-" (swap! n inc) ".edn")))
     :jsonl (fn []
              (jsonl/create! (str dir "/j-" (swap! n inc) ".jsonl")))}))

(defn- populate [store events]
  (reduce (fn [store event]
            (let [result (store/append store event)]
              (is (= :appended (:admission/verdict result))
                  (pr-str (:event/id event)))
              (:store result)))
          store
          events))

(defn- release-all [stores]
  (doseq [store stores] (store/release store)))

(defn- check-pairs [make events]
  (let [names (vec (sort (keys make)))]
    (doseq [from names
            to names]
      (let [source ((get make from))
            target ((get make to))
            filled (populate source events)
            moved (store/transfer filled target)]
        (try
          (samples/assert-preserved events (store/all-events moved) sha)
          (is (= (store/head-hash filled) (store/head-hash moved))
              (str from " -> " to))
          (is (= (domain/export-events events) (store/all-events filled)))
          (finally
            (release-all [filled source target moved])))))))

(defn- check-triples [make events]
  (let [names (vec (sort (keys make)))]
    (doseq [a names b names c names]
      (let [origin ((get make a))
            mid ((get make b))
            end ((get make c))
            direct-target ((get make c))
            filled (populate origin events)
            via (store/transfer (store/transfer filled mid) end)
            direct (store/transfer filled direct-target)]
        (try
          (is (= (store/all-events via) (store/all-events direct))
              (str a " -> " b " -> " c))
          (finally
            (release-all [origin filled mid end direct-target via direct])))))))

(defn- check-admission [make]
  (let [events (samples/plain-events sha)
        twin (second events)]
    (doseq [[name open] make]
      (let [store (populate (open) events)
            wrong (update (event-hash/seal twin sha) :event/hash
                          #(str (if (= \a (first %)) \b \a) (subs % 1)))
            rival (assoc (dissoc twin :event/hash) :event/id (samples/event-id 14))
            again (store/append store twin)
            clash (store/append store (assoc (dissoc twin :event/hash) :event/data {:changed true}))
            slot (store/append store rival)
            bad (store/append store wrong)]
        (try
          (is (= :already-present (:admission/verdict again)) name)
          (is (= :id-collision (:admission/verdict clash)) name)
          (is (= :stream-slot-conflict (:admission/verdict slot)) name)
          (is (= :rejected (:admission/verdict bad)) name)
          (is (= :clio.ledger/event-hash-mismatch (:admission/reason bad)) name)
          (is (= (domain/export-events events) (store/all-events store)) name)
          (finally
            (store/release store)))))))

(deftest every-adapter-pair-preserves-the-ledger
  (let [dir (temp-dir)]
    (try
      (let [make (factories dir)
            ledgers [(samples/rich-events sha)
                     (samples/plain-events sha)
                     (samples/gapped-events)]]
        (doseq [events ledgers]
          (check-pairs make events))
        (check-triples make (samples/rich-events sha))
        (check-admission make))
      (finally
        (fs/remove-tree! dir)))))

(deftest edn-lines-are-the-existing-ledger-format
  (let [dir (temp-dir)
        path (str dir "/events.edn")
        events (samples/rich-events sha)]
    (try
      (let [store (populate (edn-lines/create! path) (domain/export-events events))]
        (is (= (domain/export-events events) (ledger/read-ledger path)))
        (is (= (domain/export-events events)
               (store/all-events (edn-lines/open path))))
        (store/release store))
      (finally
        (fs/remove-tree! dir)))))

(deftest export-and-import-round-trip
  (let [dir (temp-dir)
        events (samples/rich-events sha)
        source (populate (memory/store) events)]
    (try
      (let [edn-store (store/import-ledger (edn-lines/create! (str dir "/in.edn"))
                                           (store/export-ledger source))
            json-store (store/import-jsonl (jsonl/create! (str dir "/in.jsonl"))
                                           (store/export-jsonl source))]
        (samples/assert-preserved events (store/all-events edn-store) sha)
        (samples/assert-preserved events (store/all-events json-store) sha)
        (store/release edn-store)
        (store/release json-store))
      (finally
        (store/release source)
        (fs/remove-tree! dir)))))
