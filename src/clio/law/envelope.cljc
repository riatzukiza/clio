(ns clio.law.envelope
  "Optional standard envelope on top of event identity.

   None of these keys is required. A ledger event written before this
   vocabulary — `:event/data` as the payload, `:event/at` an ISO string, no
   `:event/hash` — stays valid. When a key is present it has one spelling, so
   a later store cannot widen it on the way through."
  (:require [clio.law.event :as event]))

(def hash-pattern
  "Lowercase SHA-256 hex. The digest adapters emit is lowercase, and a second
   spelling of the same bits would make equality depend on the store."
  #"[0-9a-f]{64}")

(defn hash-string?
  [value]
  (and (string? value)
       (boolean (re-matches hash-pattern value))))

(defn event-at?
  "The catalog envelope stores wall time as text. A portable instant is also
   a legal `:event/at`; both are preserved, neither is rewritten."
  [value]
  (or (event/non-blank-string? value)
      (inst? value)))

(defn field-error
  "The envelope key that fails its spelling, or nil when every present
   optional key is well-formed. Absent keys are skipped."
  [event]
  (cond
    (and (contains? event :event/type)
         (not (keyword? (:event/type event))))
    :clio.ledger/invalid-type

    (and (contains? event :event/actor)
         (not (event/non-blank-string? (:event/actor event))))
    :clio.ledger/invalid-actor

    (and (contains? event :event/at)
         (not (event-at? (:event/at event))))
    :clio.ledger/invalid-at

    (and (contains? event :event/hash)
         (not (hash-string? (:event/hash event))))
    :clio.ledger/invalid-hash

    :else nil))

(defn fields-valid?
  [event]
  (and (map? event)
       (nil? (field-error event))))
