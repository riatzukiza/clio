(ns clio.law.envelope-test
  (:require [clio.law.envelope :as envelope]
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer [deftest is]])))

(def compatible
  {:event/id "11111111-1111-4111-8111-111111111111"
   :event/type :post/published
   :event/stream "post:p1"
   :event/seq 1
   :event/causes []
   :event/actor "user:alice"
   :event/at "2026-08-09T00:00:00.000Z"
   :event/data {:text "hello"}})

(deftest current-events-stay-valid
  (is (nil? (envelope/field-error compatible)))
  (is (envelope/fields-valid? compatible))
  (is (envelope/fields-valid? (dissoc compatible :event/type :event/actor :event/at))))

(deftest present-envelope-keys-keep-one-spelling
  (is (envelope/fields-valid?
       (assoc compatible
              :event/at #inst "2026-08-09T00:00:00.000Z"
              :event/body {:text "hello"}
              :event/hash (apply str (repeat 64 "a")))))
  (is (= :clio.ledger/invalid-type
         (envelope/field-error (assoc compatible :event/type "post/published"))))
  (is (= :clio.ledger/invalid-actor
         (envelope/field-error (assoc compatible :event/actor "  "))))
  (is (= :clio.ledger/invalid-at
         (envelope/field-error (assoc compatible :event/at 1))))
  (is (= :clio.ledger/invalid-hash
         (envelope/field-error (assoc compatible :event/hash (apply str (repeat 64 "A"))))))
  (is (= :clio.ledger/invalid-hash
         (envelope/field-error (assoc compatible :event/hash "abcd")))))
