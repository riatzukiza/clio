(ns clio.infra.store.jsonl
  "JSONL ledger. Each line is one tagged JSON document from `clio.shape.json`.
   The admission law is the same one the EDN-lines adapter uses."
  (:require [clio.infra.store.line-file :as line-file]))

(defn create! [path] (line-file/create-jsonl path))

(defn open [path] (line-file/open-jsonl path))
