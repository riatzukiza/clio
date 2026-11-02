(ns clio.shape.json
  "Tagged, total mapping between portable EDN values and JSON text.

   JSON has no keywords, symbols, sets, instants or UUIDs, and its numbers
   are IEEE-754 doubles. Every portable value is therefore wrapped in a tag
   array so the encoding is injective. Integers travel as decimal digits.
   Finite doubles travel as the same sign, mantissa and binary exponent
   `canonical-form` uses, and a triple that is not an exact double is refused.
   Nothing in this namespace prints a shortened number or drops a duplicate
   key or set element."
  (:require [clio.shape.canonical :as canonical]
            [clio.shape.json-text :as json-text]
            [clio.shape.time :as time]
            [clojure.string :as str]))

(def ^:private max-safe-integer 9007199254740991)

(defn- refuse!
  [message data]
  (throw (ex-info message (merge {:clio/error :clio.json/unsupported-value} data))))

(defn- bad-tag!
  [tagged message]
  (throw (ex-info message
                  {:clio/error :clio.json/invalid-tag
                   :tag (when (sequential? tagged) (first tagged))})))

(defn- arity!
  [tagged n]
  (when-not (and (vector? tagged) (= n (count tagged)))
    (bad-tag! tagged "JSON tag has the wrong number of fields")))

(defn- encode-number
  [value]
  (canonical/canonical-form value)
  (if (integer? value)
    ["integer" (str value)]
    (let [[sign digits exponent] (canonical/finite-real-components value)]
      ["real" sign digits exponent])))

(defn- order-key
  "Total order for set elements and map keys. Canonical text alone ties an
   integer with the double of the same magnitude; the type rank splits them
   so the JSON text does not depend on set iteration order."
  [value]
  [(canonical/canonical-edn value)
   (cond
     (integer? value) 0
     (number? value) 1
     (string? value) 2
     (keyword? value) 3
     (symbol? value) 4
     (uuid? value) 5
     (inst? value) 6
     (boolean? value) 7
     (nil? value) 8
     (map? value) 9
     (set? value) 10
     (vector? value) 11
     :else 12)
   (str (type value))])

(defn- encode*
  [value]
  (cond
    (record? value)
    (refuse! "Records are not part of the portable JSON mapping"
             {:value-type (str (type value))})

    (nil? value) ["nil"]
    (boolean? value) ["bool" value]
    (string? value) ["string" value]
    (keyword? value) ["keyword" (namespace value) (name value)]
    (symbol? value) ["symbol" (namespace value) (name value)]
    (uuid? value) ["uuid" (str/lower-case (str value))]
    (inst? value) ["inst" (str (inst-ms value))]
    (number? value) (encode-number value)

    (map? value)
    ["map" (mapv (fn [[k v]] [(encode* k) (encode* v)])
                 (sort-by #(order-key (first %)) value))]

    (set? value)
    ["set" (mapv encode* (sort-by order-key value))]

    (vector? value)
    ["vector" (mapv encode* value)]

    (sequential? value)
    ["vector" (mapv encode* value)]

    :else
    (refuse! "Value has no portable JSON encoding"
             {:value-type (str (type value))})))

(defn encode
  "Portable EDN value to tagged carrier data: vectors, strings, safe integers,
   booleans and nil. Refuses values `canonical-form` refuses, plus records.
   Metadata is not encoded; Clojure `=` does not read it, so a round trip
   still holds and the decoded value carries none."
  [value]
  (canonical/canonical-form value)
  (encode* value))

(declare decode)

(defn- decode-integer
  [digits]
  (let [n (when (string? digits) (parse-long digits))]
    (when-not (and n
                   (<= (- max-safe-integer) n max-safe-integer)
                   (= digits (str n)))
      (bad-tag! ["integer" digits] "Integer tag is not a canonical safe integer"))
    n))

(defn- decode-real
  [[_ sign digits exponent]]
  (when-not (and (#{1 -1} sign) (string? digits) (integer? exponent))
    (bad-tag! ["real" sign digits exponent] "Real tag is not an IEEE-754 triple"))
  (canonical/compose-real sign digits exponent))

(defn- decode-named
  [tag ns name]
  (when-not (and (or (nil? ns) (string? ns)) (string? name))
    (bad-tag! [tag ns name] "Identifier tag is not a namespace and a name"))
  (let [value (if (nil? ns) ((if (= tag "keyword") keyword symbol) name)
                 ((if (= tag "keyword") keyword symbol) ns name))]
    (canonical/canonical-form value)
    value))

(defn- decode-uuid
  [text]
  (when-not (and (string? text)
                 (re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" text))
    (bad-tag! ["uuid" text] "UUID tag is not lowercase RFC-4122 text"))
  (let [value #?(:clj (java.util.UUID/fromString text)
                 :cljs (uuid text))]
    (when-not (= text (str/lower-case (str value)))
      (bad-tag! ["uuid" text] "UUID tag does not round-trip"))
    value))

(defn- decode-inst
  [digits]
  (let [millis (when (string? digits) (parse-long digits))]
    (when-not (and millis (= digits (str millis)))
      (bad-tag! ["inst" digits] "Instant tag is not canonical millisecond digits"))
    (let [value (time/inst-from-millis millis)]
      (canonical/canonical-form value)
      value)))

(defn- decode-set
  [items]
  (let [decoded (mapv decode items)]
    (when-not (= (count decoded) (count (set decoded)))
      (throw (ex-info "JSON set repeats an element; refusing to drop it"
                      {:clio/error :clio.json/duplicate-element})))
    (set decoded)))

(defn- decode-map
  [pairs]
  (let [decoded (mapv (fn [pair]
                        (when-not (and (sequential? pair) (= 2 (count pair)))
                          (bad-tag! pair "Map entry is not a pair"))
                        [(decode (first pair)) (decode (second pair))])
                      pairs)]
    (reduce (fn [acc [k v]]
              (when (contains? acc k)
                (throw (ex-info "JSON map repeats a key; refusing to drop a value"
                                {:clio/error :clio.json/duplicate-key})))
              (assoc acc k v))
            {}
            decoded)))

(defn decode
  "Inverse of `encode` on carrier data. Unknown tags, wrong arities, duplicate
   keys and duplicate set elements are refused."
  [tagged]
  (when-not (and (vector? tagged) (string? (first tagged)))
    (bad-tag! tagged "JSON value is not a tag array"))
  (let [value (case (first tagged)
                "nil" (do (arity! tagged 1) nil)
                "bool" (do (arity! tagged 2)
                           (when-not (boolean? (second tagged))
                             (bad-tag! tagged "Boolean tag is not a boolean"))
                           (second tagged))
                "string" (do (arity! tagged 2)
                             (when-not (string? (second tagged))
                               (bad-tag! tagged "String tag is not a string"))
                             (canonical/canonical-form (second tagged))
                             (second tagged))
                "keyword" (do (arity! tagged 3)
                              (decode-named "keyword" (second tagged) (nth tagged 2)))
                "symbol" (do (arity! tagged 3)
                             (decode-named "symbol" (second tagged) (nth tagged 2)))
                "uuid" (do (arity! tagged 2) (decode-uuid (second tagged)))
                "inst" (do (arity! tagged 2) (decode-inst (second tagged)))
                "integer" (do (arity! tagged 2) (decode-integer (second tagged)))
                "real" (do (arity! tagged 4) (decode-real tagged))
                "vector" (do (arity! tagged 2)
                             (when-not (sequential? (second tagged))
                               (bad-tag! tagged "Vector tag payload is not an array"))
                             (mapv decode (second tagged)))
                "set" (do (arity! tagged 2)
                          (when-not (sequential? (second tagged))
                            (bad-tag! tagged "Set tag payload is not an array"))
                          (decode-set (second tagged)))
                "map" (do (arity! tagged 2)
                          (when-not (sequential? (second tagged))
                            (bad-tag! tagged "Map tag payload is not an array"))
                          (decode-map (second tagged)))
                (bad-tag! tagged "Unknown JSON tag"))]
    (canonical/canonical-form value)
    value))

(defn to-json
  "Encode a portable value as one JSON document."
  [value]
  (json-text/write-text (encode value)))

(defn from-json
  "Decode one JSON document. The whole text is one value."
  [text]
  (decode (json-text/read-text text)))
