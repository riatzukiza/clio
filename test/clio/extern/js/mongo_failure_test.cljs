(ns clio.extern.js.mongo-failure-test
  (:require [clio.extern.js.mongo :as mongo]
            [clio.infra.store.mongo :as store]
            [cljs.test :refer [deftest is]]))

(deftest a-driver-boot-failure-is-not-an-absent-server
  (let [code (mongo/failure-error {:err "driver" :message "synthetic setup failure"})]
    (is (= :clio.mongo/driver code))
    (is (not (store/skippable-open? code))
        "a synthetic setup failure must not take the unavailable skip")))

(deftest an-absent-server-stays-skippable
  (is (= :clio.mongo/unavailable (mongo/failure-error {:err "unavailable" :message "ECONNREFUSED"})))
  (is (store/skippable-open? :clio.mongo/unavailable))
  (is (store/skippable-open? :clio.mongo/driver-missing)))
