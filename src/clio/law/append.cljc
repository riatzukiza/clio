(ns clio.law.append
  "The explicit recovery input preserves a candidate, without certifying admission."
  (:require [malli.core :as m]))

(def recovery
  "`:ledger/segment-directory` marks a segment candidate: its retry keeps
   segment-only admission (one event per `<event-id>.edn`) instead of ordinary
   append, and `:ledger/path` must be that event's own segment."
  [:map {:closed true}
   [:ledger/path [:string {:min 1}]]
   [:ledger/segment-directory {:optional true} [:string {:min 1}]]
   [:event :map]])

(defn validate-recovery!
  [value]
  (when-not (m/validate recovery value)
    (throw (ex-info "Append recovery requires a ledger path and the exact event"
                    {:clio/error :clio.runtime/invalid-append-recovery
                     :explain (m/explain recovery value)})))
  value)
