(ns clio.domain.store
  "Ledger shape over an immutable collection of events.

   Append uses `clio.law.ledger/append-admission` unchanged. Identity,
   the optional envelope and the content hash are checked first, and a
   failure of those is `:rejected` rather than a new admission verdict.
   Causal closure and contiguous sequences stay in canonicalization: a
   store may hold one partition of a larger history."
  (:require [clio.domain.event-hash :as event-hash]
            [clio.law.envelope :as envelope]
            [clio.law.event :as event]
            [clio.law.ledger :as ledger]
            [clio.shape.canonical :as canonical]))

(defn- rejected
  [reason]
  {:admission/verdict :rejected
   :admission/reason reason})

(defn admit
  "Classify `event` against `events`.

   The four law verdicts are returned as `append-admission` produces them.
   Identity, envelope and hash failures return `:rejected` and do not consult
   the slot table. A non-portable payload is `:rejected` with the canonical
   error as `:admission/reason`."
  [events event hash-fn]
  (cond
    (not (map? event))
    (rejected :clio.ledger/not-a-map)

    (not (event/event-identity-valid? event))
    (rejected :clio.ledger/invalid-identity)

    (envelope/field-error event)
    (rejected (envelope/field-error event))

    :else
    (try
      (if (event-hash/hash-matches? event hash-fn)
        (ledger/append-admission events event)
        (rejected :clio.ledger/event-hash-mismatch))
      (catch #?(:clj Exception :cljs :default) cause
        (rejected (or (:clio/error (ex-data cause))
                      :clio.ledger/non-portable))))))

(defn apply-admit
  "Admit `event`. On `:appendable`, conjoin it. Otherwise return `events`
   unchanged, with the verdict."
  [events event hash-fn]
  (let [verdict (admit events event hash-fn)]
    (assoc verdict
           :events
           (if (= :appendable (:admission/verdict verdict))
             (conj (vec events) event)
             (vec events)))))

(defn export-order
  "Events grouped by stream. Stream ids are sorted by `compare`. Each group
   is sorted by `:event/seq`."
  [events]
  (let [grouped (group-by :event/stream events)]
    (mapv (fn [stream]
            (vec (sort-by :event/seq (get grouped stream))))
          (sort (keys grouped)))))

(defn export-events
  "Stream-major concatenation of `export-order`. This is the order transfer
   appends, independent of the order the events were stored."
  [events]
  (vec (mapcat identity (export-order events))))

(defn streams
  [events]
  (vec (sort (distinct (map :event/stream events)))))

(defn read-range
  "Events in `stream` with `:event/seq` in the inclusive interval
   [`from`, `to`]. A missing `:to` means no upper bound. `:from` defaults
   to 1. The result is ordered by sequence."
  [events stream opts]
  (let [opts (if (map? opts) opts
                 (throw (ex-info "read-range options must be a map"
                                 {:clio/error :clio.ledger/invalid-range})))
        from (:from opts 1)
        to (:to opts)]
    (when-not (event/non-blank-string? stream)
      (throw (ex-info "read-range requires a stream id"
                      {:clio/error :clio.ledger/invalid-stream})))
    (when-not (and (event/positive-int? from)
                   (or (nil? to) (event/positive-int? to)))
      (throw (ex-info "read-range bounds must be positive integers"
                      {:clio/error :clio.ledger/invalid-range
                       :from from
                       :to to})))
    (->> events
         (filter (fn [candidate]
                   (and (= stream (:event/stream candidate))
                        (<= from (:event/seq candidate))
                        (or (nil? to) (<= (:event/seq candidate) to)))))
         (sort-by :event/seq)
         vec)))

(defn- chain
  [ordered hash-fn]
  (reduce (fn [acc item]
            (hash-fn (canonical/canonical-edn
                      [acc (event-hash/event-hash item hash-fn)])))
          (hash-fn (canonical/canonical-edn [:clio.ledger/empty-head]))
          ordered))

(defn head-hash
  "Digest of the stream family.

   Each stream is chained in sequence order from a fixed empty-head preimage.
   The ledger digest is the canonical hash of the sorted map of those tips.
   Gaps are not filled: a missing sequence is absent from the chain, which
   is why this is a store tip and not a canonicalized history."
  [events hash-fn]
  (let [tips (mapv (fn [group]
                     [(:event/stream (first group)) (chain group hash-fn)])
                   (export-order events))]
    (hash-fn (canonical/canonical-edn (into (sorted-map) tips)))))
