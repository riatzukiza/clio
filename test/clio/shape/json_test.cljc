(ns clio.shape.json-test
  (:require [clio.shape.canonical :as canonical]
            [clio.shape.json :as json]
            [clio.shape.json-text :as json-text]
            [clio.shape.time :as time]
            #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing]])))

(defn- error [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) cause
         (:clio/error (ex-data cause)))))

(def corpus
  [nil
   true
   false
   ""
   "hello \u03bb"
   "a\nb\t\"\\"
   "\uD83D\uDE00"
   :a
   :event/id
   (keyword "music" "\u03bb")
   'ledger
   'clio.ledger/empty-head
   #uuid "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
   #inst "1970-01-01T00:00:00.000Z"
   #inst "1969-12-31T23:59:59.999Z"
   #inst "2026-08-09T00:00:00.000Z"
   #inst "1582-10-15T00:00:00.000Z"
   #inst "9999-12-31T23:59:59.999Z"
   0
   1
   -1
   9007199254740991
   -9007199254740991
   0.0
   -0.0
   1.0
   -1.0
   0.1
   -2.75
   1.5
   []
   [1 :a]
   '(1 2)
   #{}
   #{:a :b/c 1 "x"}
   {}
   {:b 1 :a 2}
   {:event/id "x" :nested {:k #{[1]}}}
   {[1 :a] {:b #{:c}}}])

(deftest values-round-trip-and-keep-their-canonical-form
  (doseq [value corpus]
    (testing (pr-str value)
      (let [text (json/to-json value)
            back (json/from-json text)]
        (is (= value back))
        (is (= (canonical/canonical-form value) (canonical/canonical-form back)))
        (is (= text (json-text/write-text (json-text/read-text text))))
        (is (= text (json/to-json back)))))))

(deftest object-key-order-does-not-change-the-text
  (is (= (json/to-json {:b 1 :a {:d 1 :c 2}})
         (json/to-json {:a {:c 2 :d 1} :b 1})))
  (is (= (json/to-json #{3 1 2})
         (json/to-json #{2 3 1}))))

(deftest integers-and-doubles-are-not-rounded
  (is (= 9007199254740991 (json/from-json (json/to-json 9007199254740991))))
  (is (= 0.1 (json/from-json (json/to-json 0.1))))
  (is (= (canonical/canonical-edn 1.0)
         (canonical/canonical-edn (json/from-json (json/to-json 1.0)))))
  #?(:clj
     (is (double? (json/from-json (json/to-json 1.0)))
         "the JVM distinguishes 1.0 from 1, and the codec keeps the double")))

(deftest non-portable-values-are-refused
  (is (= :clio.canonical/non-portable-number (error #(json/to-json ##NaN))))
  (is (= :clio.canonical/non-portable-number (error #(json/to-json ##Inf))))
  (is (= :clio.canonical/non-portable-number (error #(json/to-json 1e20))))
  (is (= :clio.canonical/invalid-unicode (error #(json/to-json "\uD800"))))
  (is (= :clio.canonical/invalid-identifier (error #(json/to-json (keyword "bad key")))))
  (let [tagged (with-meta {:a 1} {:m true})]
    (is (= tagged (json/from-json (json/to-json tagged))))
    (is (nil? (meta (json/from-json (json/to-json tagged))))
        "metadata is not part of the mapping; = does not read it"))
  #?(:clj
     (do
       (is (= :clio.canonical/non-portable-number (error #(json/to-json 1/2))))
       (is (= :clio.canonical/non-portable-number
              (error #(json/to-json (+ 9007199254740991 2))))))))

(deftest malformed-json-is-not-a-prefix
  (is (= :clio.json/invalid-text (error #(json/from-json ""))))
  (is (= :clio.json/invalid-text (error #(json/from-json (str (json/to-json {:a 1}) "x")))))
  (is (= :clio.json/invalid-text (error #(json/from-json "[1.5]"))))
  (is (= :clio.json/invalid-text (error #(json/from-json "{\"a\":1}"))))
  (is (= :clio.json/invalid-text (error #(json-text/read-text "99999999999999999999"))))
  (is (= :clio.json/invalid-tag
         (error #(json/from-json "[\"integer\",\"99999999999999999999\"]"))))
  (is (= :clio.json/invalid-text
         (error #(json/from-json (subs (json/to-json {:a 1}) 0 8))))))

(deftest tags-do-not-drop-duplicates-or-unknowns
  (is (= :clio.json/duplicate-key
         (error #(json/decode ["map" [[["keyword" nil "a"] ["integer" "1"]]
                                      [["keyword" nil "a"] ["integer" "2"]]]]))))
  (is (= :clio.json/duplicate-element
         (error #(json/decode ["set" [["integer" "1"] ["integer" "1"]]]))))
  (is (= :clio.json/invalid-tag (error #(json/decode ["nope"]))))
  (is (= :clio.json/invalid-tag (error #(json/decode ["integer" "01"]))))
  (is (= :clio.json/invalid-tag (error #(json/decode ["uuid" "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE"]))))
  (is (= :clio.canonical/non-portable-number
         (error #(json/decode ["real" 1 "4503599627370496" 9007199254740991])))
      "an exponent outside binary64 must be refused before reconstruction"))

(defrecord SampleBox [x])

(deftest records-are-refused
  (is (= :clio.json/unsupported-value (error #(json/to-json (->SampleBox 1))))))

(deftest instants-use-the-utc-text-the-edn-reader-uses
  (doseq [[inst literal] [[#inst "1970-01-01T00:00:00.000Z" "1970-01-01T00:00:00.000Z"]
                          [#inst "1969-12-31T23:59:59.999Z" "1969-12-31T23:59:59.999Z"]
                          [#inst "2026-08-09T00:00:00.000Z" "2026-08-09T00:00:00.000Z"]
                          [#inst "1582-10-15T00:00:00.000Z" "1582-10-15T00:00:00.000Z"]
                          [#inst "9999-12-31T23:59:59.999Z" "9999-12-31T23:59:59.999Z"]]]
    (is (= literal (time/millis->inst-text (inst-ms inst))))
    (is (= (inst-ms inst) (inst-ms (time/inst-from-millis (inst-ms inst)))))
    (is (= inst (json/from-json (json/to-json inst))))))
