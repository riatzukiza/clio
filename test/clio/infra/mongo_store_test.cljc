(ns clio.infra.mongo-store-test
  (:require [clio.domain.store :as domain]
            [clio.infra.store :as store]
            [clio.infra.store.edn-lines :as edn-lines]
            [clio.infra.store.jsonl :as jsonl]
            [clio.infra.store.memory :as memory]
            [clio.infra.store.mongo :as mongo]
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

(defn- populate [store events]
  (reduce (fn [store event]
            (let [result (store/append store event)]
              (is (= :appended (:admission/verdict result)))
              (:store result)))
          store
          events))

(defn- release-all [stores]
  (doseq [store stores]
    (when (some? store) (store/release store))))

(defn- mongo-open [uri]
  (fn []
    (let [opened (mongo/connect {:uri uri
                                 :collection (str "clio-" (runtime/random-uuid))
                                 :drop-on-release? true})]
      (when-not (:store opened)
        (throw (ex-info "MongoDB became unavailable during transfer"
                        opened)))
      (:store opened))))

(defn- probe []
  (let [uri (or (mongo/configured-uri) "mongodb://127.0.0.1:27017")
        opened (mongo/connect {:uri uri
                               :collection (str "clio-probe-" (runtime/random-uuid))
                               :drop-on-release? true})]
    (if-let [store (:store opened)]
      (do (store/release store)
          {:uri uri})
      {:skipped (:skipped opened) :message (:message opened)})))

(deftest a-driver-failure-is-not-a-skip
  (is (not (mongo/skippable-open? :clio.mongo/driver)))
  (is (mongo/skippable-open? :clio.mongo/unavailable))
  (is (mongo/skippable-open? :clio.mongo/driver-missing)))

(deftest mongo-transfer-pairs
  (let [configured (mongo/configured-uri)
        probed (probe)]
    (cond
      (and configured (:skipped probed))
      (is false
          (str "Configured Mongo endpoint failed qualification: "
               (:skipped probed) " " (:message probed)))

      :else
      (if-let [uri (:uri probed)]
      (let [dir (str "/tmp/clio-mongo-" (runtime/random-uuid))
            _ (fs/ensure-dir! dir)
            n (atom 0)
            make {:memory (fn [] (memory/store))
                  :edn (fn [] (edn-lines/create! (str dir "/e-" (swap! n inc) ".edn")))
                  :jsonl (fn [] (jsonl/create! (str dir "/j-" (swap! n inc) ".jsonl")))
                  :mongo (mongo-open uri)}
            events (samples/rich-events sha)
            names (vec (sort (keys make)))]
        (try
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
                (is (= (domain/export-events events) (store/all-events moved)))
                (finally
                  (release-all [source filled target moved])))))
          (let [origin ((get make :memory))
                mid ((get make :mongo))
                end ((get make :jsonl))
                direct-target ((get make :jsonl))
                filled (populate origin events)
                via (store/transfer (store/transfer filled mid) end)
                direct (store/transfer filled direct-target)]
            (try
              (is (= (store/all-events via) (store/all-events direct))
                  "memory -> mongo -> jsonl equals memory -> jsonl")
              (finally
                (release-all [origin filled mid end direct-target via direct]))))
          (let [store ((get make :mongo))
                twin (second events)
                refused (store/append store (assoc (dissoc twin :event/hash) :event/seq 0))]
            (try
              (is (= :rejected (:admission/verdict refused)))
              (is (= :clio.ledger/invalid-identity (:admission/reason refused))
                  "a non-positive sequence is an identity refusal, not a driver error")
              (finally
                (store/release store))))
          (finally
            (fs/remove-tree! dir))))
      (do
        (println "clio mongo transfer: SKIP"
                 (pr-str (:skipped probed))
                 (:message probed))
        (is (#{:clio.mongo/unavailable :clio.mongo/driver-missing} (:skipped probed))
            "an absent MongoDB driver or server skips transfer instead of failing"))))))
