(ns clio.domain.event-hash
  "Content hash of one event.

   The preimage is the canonical EDN of the event with `:event/hash` removed.
   The stored digest is therefore not an input to itself, and an event that
   never carried the key hashes the same as its sealed copy."
  (:require [clio.shape.canonical :as canonical]))

(defn preimage
  [event]
  (canonical/canonical-edn (dissoc event :event/hash)))

(defn event-hash
  "SHA-256, as supplied by `hash-fn`, of `preimage`."
  [event hash-fn]
  (hash-fn (preimage event)))

(defn hash-matches?
  "True when the envelope hash is absent or equal to `event-hash`.
   Computing the digest still refuses a non-portable payload."
  [event hash-fn]
  (let [computed (event-hash event hash-fn)]
    (or (not (contains? event :event/hash))
        (= (:event/hash event) computed))))

(defn seal
  "Associate `:event/hash` when it is absent. A present digest must already
   be the preimage hash; this does not overwrite a disagreement."
  [event hash-fn]
  (let [computed (event-hash event hash-fn)
        stored (:event/hash event)]
    (cond
      (nil? stored) (assoc event :event/hash computed)
      (= stored computed) event
      :else
      (throw (ex-info "Stored event hash does not match the canonical preimage"
                      {:clio/error :clio.event/hash-mismatch
                       :event/hash stored
                       :computed/hash computed})))))
