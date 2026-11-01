(ns clio.infra.store.edn-lines
  "EDN-lines ledger. Each line is one event printed with `pr-str`, which is
   the file format `clio.infra.ledger` already writes. `create!` is the only
   way to bring the file into existence."
  (:require [clio.infra.store.line-file :as line-file]))

(defn create! [path] (line-file/create-edn path))

(defn open [path] (line-file/open-edn path))
