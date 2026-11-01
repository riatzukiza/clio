#!/usr/bin/env bb

(require '[clojure.test :as test]
         '[clio.domain.event-hash-test]
         '[clio.domain.store-test]
         '[clio.law.envelope-test]
         '[clio.law.ledger-test]
         '[clio.lint-extern-boundary-test]
         '[clio.shape.canonical-test]
         '[clio.shape.json-test])

(let [{:keys [fail error]}
      (test/run-tests 'clio.shape.canonical-test
                      'clio.shape.json-test
                      'clio.domain.event-hash-test
                      'clio.domain.store-test
                      'clio.law.envelope-test
                      'clio.law.ledger-test
                      'clio.lint-extern-boundary-test)]
  (when (pos? (+ fail error))
    (System/exit 1)))
