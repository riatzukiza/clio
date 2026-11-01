(ns clio.shape.json-text
  "JSON text for the tagged codec's carrier.

   The carrier is arrays, strings, integers, booleans and null. Floats are
   rejected, not rounded: a `.` or exponent marker is a malformed document.
   Integers that do not fit in a 64-bit signed value are rejected, not
   narrowed. Trailing characters are rejected, not ignored."
  (:require [clojure.string :as str]))

(defn- fail!
  [s i message]
  (throw (ex-info message
                  {:clio/error :clio.json/invalid-text
                   :offset i
                   :length (count s)})))

(defn- unit [s i]
  #?(:clj (int (.charAt ^String s i))
     :cljs (.charCodeAt s i)))

(defn- skip [s i]
  (loop [i i]
    (if (and (< i (count s))
             (let [u (unit s i)]
               (or (= u 32) (= u 9) (= u 10) (= u 13))))
      (recur (inc i))
      i)))

(defn- hex [c]
  (cond
    (<= 48 c 57) (- c 48)
    (<= 97 c 102) (- c 87)
    (<= 65 c 70) (- c 55)
    :else nil))

(defn- read-hex [s i]
  (loop [n 0 k 0 i i]
    (if (= k 4)
      [n i]
      (if (>= i (count s))
        (fail! s i "Truncated JSON unicode escape")
        (if-let [d (hex (unit s i))]
          (recur (+ (* n 16) d) (inc k) (inc i))
          (fail! s i "JSON unicode escape is not four hex digits"))))))

(defn- push-unit [parts code]
  (conj parts (str (char code))))

(defn- read-string* [s i]
  (loop [i (inc i) parts []]
    (if (>= i (count s))
      (fail! s i "Unclosed JSON string")
      (let [u (unit s i)]
        (cond
          (= u 34) [(apply str parts) (inc i)]
          (= u 92)
          (if (>= (inc i) (count s))
            (fail! s i "Truncated JSON escape")
            (let [e (unit s (inc i))]
              (case e
                34 (recur (+ i 2) (conj parts "\""))
                92 (recur (+ i 2) (conj parts "\\"))
                47 (recur (+ i 2) (conj parts "/"))
                98 (recur (+ i 2) (conj parts "\b"))
                102 (recur (+ i 2) (conj parts "\f"))
                110 (recur (+ i 2) (conj parts "\n"))
                114 (recur (+ i 2) (conj parts "\r"))
                116 (recur (+ i 2) (conj parts "\t"))
                117 (let [[code j] (read-hex s (+ i 2))]
                      (recur j (push-unit parts code)))
                (fail! s i "Unknown JSON string escape"))))
          (< u 32) (fail! s i "Unescaped control character in JSON string")
          :else (recur (inc i) (push-unit parts u)))))))

(defn- read-int* [s i]
  (let [n (count s)
        neg? (= (unit s i) 45)
        start (if neg? (inc i) i)]
    (when (or (>= start n) (< (unit s start) 48) (> (unit s start) 57))
      (fail! s i "JSON number is missing digits"))
    (when (and (= (unit s start) 48) (< (inc start) n)
               (<= 48 (unit s (inc start)) 57))
      (fail! s i "JSON number has a leading zero"))
    (loop [j start]
      (if (and (< j n) (<= 48 (unit s j) 57))
        (recur (inc j))
        (let [u (when (< j n) (unit s j))]
          (when (and u (or (= u 46) (= u 101) (= u 69)))
            (fail! s j "JSON floats are not in the tagged carrier"))
            (let [digits (subs s start j)
                parsed (parse-long (if neg? (str "-" digits) digits))
                max-safe 9007199254740991]
            (when (or (nil? parsed) (< parsed (- max-safe)) (> parsed max-safe))
              (fail! s i "JSON integer is outside the portable safe-integer domain"))
            [parsed j]))))))

(declare read-value)

(defn- read-array* [s i]
  (let [n (count s)
        i (skip s (inc i))]
    (if (and (< i n) (= (unit s i) 93))
      [[] (inc i)]
      (loop [i i items []]
        (let [[item i] (read-value s i)
              items (conj items item)
              i (skip s i)]
          (when (>= i n) (fail! s i "Unclosed JSON array"))
          (case (unit s i)
            93 [items (inc i)]
            44 (let [i (skip s (inc i))]
                 (when (or (>= i n) (= (unit s i) 93))
                   (fail! s i "Trailing comma in JSON array"))
                 (recur i items))
            (fail! s i "Expected ',' or ']' in JSON array")))))))

(defn- literal [s i text value]
  (let [end (+ i (count text))]
    (if (and (<= end (count s)) (= text (subs s i end)))
      [value end]
      (fail! s i (str "Expected JSON literal " text)))))

(defn- read-value [s i]
  (let [i (skip s i)]
    (when (>= i (count s)) (fail! s i "Unexpected end of JSON"))
    (case (unit s i)
      110 (literal s i "null" nil)
      116 (literal s i "true" true)
      102 (literal s i "false" false)
      34 (read-string* s i)
      91 (read-array* s i)
      123 (fail! s i "JSON objects are not in the tagged carrier")
      (45 48 49 50 51 52 53 54 55 56 57) (read-int* s i)
      (fail! s i "Unexpected JSON character"))))

(defn read-text
  "Read one JSON value. Whitespace may surround it. Nothing may follow it."
  [text]
  (when-not (string? text)
    (throw (ex-info "JSON text must be a string"
                    {:clio/error :clio.json/invalid-text})))
  (let [[value i] (read-value text 0)
        i (skip text i)]
    (when (< i (count text))
      (fail! text i "Trailing data after JSON value"))
    value))

(defn- pad-hex [u]
  (let [digits "0123456789abcdef"]
    (str (nth digits (mod (quot u 4096) 16))
         (nth digits (mod (quot u 256) 16))
         (nth digits (mod (quot u 16) 16))
         (nth digits (mod u 16)))))

(defn- emit-string [s]
  (let [n (count s)]
    (loop [i 0 parts ["\""]]
      (if (>= i n)
        (apply str (conj parts "\""))
        (let [u (unit s i)
              parts (cond
                      (= u 34) (conj parts "\\\"")
                      (= u 92) (conj parts "\\\\")
                      (= u 8) (conj parts "\\b")
                      (= u 12) (conj parts "\\f")
                      (= u 10) (conj parts "\\n")
                      (= u 13) (conj parts "\\r")
                      (= u 9) (conj parts "\\t")
                      (< u 32) (conj parts (str "\\u" (pad-hex u)))
                      :else (conj parts (str (char u))))]
          (recur (inc i) parts))))))

(def ^:private max-safe-integer 9007199254740991)

(defn- emit [value]
  (cond
    (nil? value) "null"
    (true? value) "true"
    (false? value) "false"
    (string? value) (emit-string value)
    (and (integer? value)
         (<= (- max-safe-integer) value max-safe-integer))
    (str (long value))
    (vector? value)
    (str "[" (str/join "," (map emit value)) "]")
    :else
    (throw (ex-info "Tagged JSON carrier cannot encode this value"
                    {:clio/error :clio.json/invalid-carrier
                     :value-type (str (type value))}))))

(defn write-text
  "Compact JSON for one carrier value. The same value always yields the same text."
  [value]
  (emit value))
