(ns clio.domain.ledger-text
  "Line morphisms for a ledger partition.

   EDN lines are `pr-str`, the format `clio.infra.ledger` already appends.
   JSON lines are the tagged codec, one document per line. Blank lines are
   ignored. A line that does not read as one value is refused with its line
   number; the following lines are not returned in its place."
  (:require [clio.shape.edn :as edn]
            [clio.shape.json :as json]
            [clojure.string :as str]))

(defn- parse-lines
  [text decode-line error]
  (when-not (string? text)
    (throw (ex-info "Ledger text must be a string"
                    {:clio/error error})))
  (->> (str/split-lines text)
       (map-indexed vector)
       (remove (fn [[_ line]] (str/blank? line)))
       (mapv (fn [[index line]]
               (try
                 (decode-line line)
                 (catch #?(:clj Exception :cljs :default) cause
                   (throw (ex-info "Ledger line did not read as one event"
                                   {:clio/error error
                                    :line (inc index)
                                    :cause (ex-message cause)}
                                   cause))))))))

(defn- emit-lines
  [events encode-line]
  (str/join (map (fn [event] (str (encode-line event) "\n")) events)))

(defn pr-event
  "One-line readable EDN. Printer limits are cleared so a bound
   `*print-length*` cannot abbreviate an event."
  [event]
  (binding [*print-length* nil
            *print-level* nil]
    (pr-str event)))

(defn emit-edn
  [events]
  (emit-lines events pr-event))

(defn parse-edn
  [text]
  (parse-lines text edn/read-one :clio.ledger/invalid-edn))

(defn emit-jsonl
  [events]
  (emit-lines events json/to-json))

(defn parse-jsonl
  [text]
  (parse-lines text json/from-json :clio.json/invalid-text))
