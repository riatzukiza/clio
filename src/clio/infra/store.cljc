(ns clio.infra.store
  "Ledger store protocol.

   `append` returns the admission verdict and the store to keep using.
   An in-memory store returns a new value. A host store returns its handle
   after the durable write, or the same handle when the law refuses the
   write. `read-range`, `streams` and `head-hash` are the observations
   transfer is defined on. Later adapters (sqlite, postgres, duckdb, redis,
   elasticsearch, opensearch) implement this protocol and nothing in
   transfer changes.

   `:from` defaults to 1 and `:to` is inclusive. Omit `:to` for an open end."
  (:require [clio.domain.ledger-text :as ledger-text]))

(defprotocol LedgerStore
  (append [store event]
    "Admit `event`. Returns `{:admission/verdict _ :store store}`.")
  (read-range [store stream opts]
    "Events of `stream` in the inclusive sequence interval.")
  (streams [store]
    "Stream ids, sorted by `compare`.")
  (head-hash [store]
    "Ledger tip digest. Equal ledgers have equal tips.")
  (release [store]
    "Release host resources. Idempotent."))

(defn all-events
  "Export order: streams by id, then each stream by `:event/seq`."
  [store]
  (into []
        (mapcat (fn [stream] (read-range store stream {})))
        (streams store)))

(defn- accepted
  [result event]
  (case (:admission/verdict result)
    (:appended :already-present) (:store result)
    (throw (ex-info "Ledger store refused the event"
                    (assoc result
                           :clio/error :clio.ledger/refused
                           :event event)))))

(defn transfer
  "`F_target ∘ F_source⁻¹`. Events are appended one at a time, in export
   order. This is not a transaction: a refusal throws and the target keeps
   the events already accepted. A caller that must commit several events
   together needs a store-specific transaction or its own envelope; this
   protocol does not provide one."
  [source target]
  (reduce (fn [target event]
            (accepted (append target event) event))
          target
          (all-events source)))

(defn export-ledger
  "Canonical export as EDN lines in export order. One `pr-str` event per line."
  [store]
  (ledger-text/emit-edn (all-events store)))

(defn import-ledger
  "Admit each EDN line into `store`, in file order."
  [store text]
  (reduce (fn [store event]
            (accepted (append store event) event))
          store
          (ledger-text/parse-edn text)))

(defn export-jsonl
  "Tagged JSON lines in export order."
  [store]
  (ledger-text/emit-jsonl (all-events store)))

(defn import-jsonl
  [store text]
  (reduce (fn [store event]
            (accepted (append store event) event))
          store
          (ledger-text/parse-jsonl text)))

(defn verdict
  "Attach `store` to an admission map, dropping keys the law did not set."
  [admission store]
  (assoc (select-keys admission [:admission/verdict
                                 :admission/conflict
                                 :admission/reason
                                 :admission/cause])
         :store store))
