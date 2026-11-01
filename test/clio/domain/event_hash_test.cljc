(ns clio.domain.event-hash-test
  (:require [clio.domain.event-hash :as event-hash]
            [clio.shape.canonical :as canonical]
            #?(:clj [clio.extern.jvm.crypto :as crypto]
               :cljs [clio.extern.js.crypto :as crypto])
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer [deftest is]])))

(defn- sha [text] (crypto/sha256 text))

(def event
  {:event/id "11111111-1111-4111-8111-111111111111"
   :event/stream "post:p1"
   :event/seq 1
   :event/causes []
   :event/type :post/published
   :event/data {:text "hello"}})

(defn- error [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) cause
         (:clio/error (ex-data cause)))))

(deftest the-hash-does-not-read-itself
  (let [sealed (event-hash/seal event sha)]
    (is (= (event-hash/event-hash event sha)
           (event-hash/event-hash sealed sha)))
    (is (= (:event/hash sealed) (event-hash/event-hash sealed sha)))
    (is (not= (canonical/canonical-edn event)
              (canonical/canonical-edn sealed))
        "the stored field is outside the preimage, so the documents differ")
    (is (= sealed (event-hash/seal sealed sha)))
    (is (event-hash/hash-matches? event sha))
    (is (event-hash/hash-matches? sealed sha))))

(deftest a-nested-hash-key-stays-in-the-preimage
  (let [nested (assoc-in event [:event/data :event/hash] "not-the-envelope")]
    (is (not= (event-hash/event-hash event sha)
              (event-hash/event-hash nested sha)))))

(deftest a-disagreement-is-not-overwritten
  (let [sealed (event-hash/seal event sha)
        wrong (update sealed :event/hash #(str (if (= \a (first %)) \b \a) (subs % 1)))]
    (is (= :clio.event/hash-mismatch (error #(event-hash/seal wrong sha))))
    (is (not (event-hash/hash-matches? wrong sha)))))
