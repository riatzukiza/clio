(ns clio.infra.file-guard-test
  (:require #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer [deftest is]])
            #?(:clj [clio.extern.jvm.fs :as fs] :cljs [clio.extern.js.fs :as fs])
            #?(:clj [clio.extern.jvm.runtime :as host] :cljs [clio.extern.js.runtime :as host])
            #?(:clj [clio.extern.jvm.file-guard-fixture :as fixture]
               :cljs [clio.extern.js.file-guard-fixture :as fixture])
            [clio.infra.runtime :as runtime]
            [clio.infra.ledger :as ledger]
            [clio.infra.host-fixture :as facts]))

(defn- outcome [f]
  (try {:value (f)} (catch #?(:clj Exception :cljs :default) cause
                     {:error (:clio/error (ex-data cause)) :message (ex-message cause)})))

(defn- with-store [f]
  (let [directory (str "/tmp/clio-file-guard-" (host/random-uuid))
        path (str directory "/events.edn") substitute (str directory "/substitute.edn")]
    (try
      (fs/ensure-dir! directory) (ledger/create-ledger! path) (ledger/create-ledger! substitute)
      (f {:directory directory :path path :substitute substitute
          :runtime (runtime/open (str directory "/schemas") facts/catalog)
          :identity (fixture/path-identity path)})
      (finally (fs/remove-tree! directory)))))

(deftest guarded-runtime-preserves-default-admission-replay-and-recovery
  (with-store
    (fn [{:keys [directory path runtime identity]}]
      (let [guard {:expected-file-identity identity}
            reads {:expected-file-identities {path identity}}
            result (outcome #(runtime/append! runtime path :record/observed facts/facts guard))]
        (is (= :appended (get-in result [:value :append/result])) (pr-str result))
        (when-let [event (get-in result [:value :event])]
          (is (= [event] (:canonical/events (runtime/canonicalize-files runtime [path] reads))))
          (is (= :already-present (:append/result (runtime/retry-append! runtime {:ledger/path path :event event} guard))))
          (is (= path (runtime/ensure-durable! runtime path guard)))
          (is (= [event] (ledger/read-ledger path)))
          (let [alias (str directory "/alias.edn")]
            (fs/hard-link! path alias)
            (is (= [event] (:canonical/events (runtime/canonicalize-files runtime [alias]
                                               {:expected-file-identities {alias identity}})))))
          (is (not (contains? event :expected-file-identity))))))))

(deftest wrong-descriptor-identity-refuses-before-any-ledger-read-or-append
  (with-store
    (fn [{:keys [path substitute runtime identity]}]
      (let [wrong (fixture/path-identity substitute) calls (atom [])
            read! fs/read-locked-text append! fs/append-locked-text!]
        (is (not= identity wrong))
        (with-redefs [fs/read-locked-text (fn [lock] (swap! calls conj :read) (read! lock))
                      fs/append-locked-text! (fn [lock text] (swap! calls conj :append) (append! lock text))]
          (doseq [operation [#(runtime/canonicalize-files runtime [path] {:expected-file-identities {path wrong}})
                             #(runtime/append! runtime path :record/observed facts/facts {:expected-file-identity wrong})
                             #(runtime/ensure-durable! runtime path {:expected-file-identity wrong})]]
            (is (= :clio.fs/file-identity-mismatch (:error (outcome operation))))))
        (is (= [] @calls) "Rejected identity must never read or append ledger bytes")
        (is (= "" (fs/read-text path)))
        (let [lock (fs/acquire-lock! path)] (fs/release-lock! lock))))))

(deftest restored-pathname-cannot-hide-the-substitute-owning-descriptor
  (doseq [operation [:read :append]]
    (with-store
      (fn [{:keys [directory path substitute runtime identity]}]
        (let [calls (atom []) read! fs/read-locked-text append! fs/append-locked-text!
              result (with-redefs [fs/read-locked-text (fn [lock] (swap! calls conj :read) (read! lock))
                                   fs/append-locked-text! (fn [lock text] (swap! calls conj :append) (append! lock text))]
                       (fixture/with-descriptor-substitution!
                         path substitute (str directory "/moved.edn")
                         #(outcome (if (= :read operation)
                                     (fn [] (runtime/canonicalize-files runtime [path] {:expected-file-identities {path identity}}))
                                     (fn [] (runtime/append! runtime path :record/observed facts/facts {:expected-file-identity identity}))))))]
          (is (true? (:swapped? result)) (pr-str result))
          (is (= :clio.fs/file-identity-mismatch (get-in result [:value :error])) (pr-str result))
          (is (= identity (fixture/path-identity path)) "Original pathname identity was restored")
          (is (= [] @calls) "Checking the pathname after open would miss this ABA")
          (is (= "" (fs/read-text substitute)))
          (doseq [p [path substitute]]
            (let [lock (fs/acquire-lock! p)] (fs/release-lock! lock))))))))

(deftest guarded-recovery-cannot-append-into-a-different-inode
  (with-store
    (fn [{:keys [path substitute runtime identity]}]
      (let [unguarded (runtime/append! runtime path :record/observed facts/facts)
            event (:event unguarded) guard {:expected-file-identity identity}
            recovered (outcome #(runtime/retry-append! runtime {:ledger/path substitute :event event} guard))]
        (is (= :clio.fs/file-identity-mismatch (:error recovered)) (pr-str recovered))
        (is (= "" (fs/read-text substitute)))
        (is (= [event] (ledger/read-ledger path)))))))

(deftest guard-declarations-are-explicit-and-malformed-identities-refuse
  (with-store
    (fn [{:keys [path runtime]}]
      (is (= :clio.fs/missing-file-identity
             (:error (outcome #(runtime/canonicalize-files runtime [path] {:expected-file-identities {}})))))
      (doseq [identity [nil [1 2] ["1"] ["1" "2" "3"] ["-1" "2"] ["1" "x"]]]
        (is (= :clio.fs/invalid-file-identity
               (:error (outcome #(runtime/append! runtime path :record/observed facts/facts
                                                  {:expected-file-identity identity}))))))
      (is (= "" (fs/read-text path))))))

(deftest malformed-canonical-guard-options-never-downgrade-to-an-unguarded-read
  (with-store
    (fn [{:keys [path runtime]}]
      (let [reads (atom 0) read! fs/read-locked-text]
        (with-redefs [fs/read-locked-text (fn [lock] (swap! reads inc) (read! lock))]
          (doseq [[options code] [[{} :clio.fs/missing-file-identity]
                                  [[] :clio.fs/invalid-file-guard-options]
                                  [1 :clio.fs/invalid-file-guard-options]
                                  [{:expected-file-identities nil} :clio.fs/invalid-file-guard-options]
                                  [{:expected-file-identities {path nil}} :clio.fs/invalid-file-identity]]]
            (is (= code (:error (outcome #(runtime/canonicalize-files runtime [path] options)))))))
        (is (zero? @reads))))))

(deftest guarded-public-descriptor-identity-and-unsupported-capability-cleanup
  (with-store
    (fn [{:keys [path identity]}]
      (doseq [acquire! [fs/acquire-read-lock! fs/acquire-lock!]]
        (let [lock (acquire! path {:expected-file-identity identity})]
          (try (is (= identity (fs/locked-file-identity lock)))
               (is (every? string? (fs/locked-file-identity lock)))
               (finally (fs/release-lock! lock))))
        (let [result (fixture/with-unavailable-identity!
                       #(outcome (fn [] (acquire! path {:expected-file-identity identity}))))]
          ;; A failed injection must still release an unexpected returned token.
          (when-let [lock (:value result)] (fs/release-lock! lock))
          (is (= fixture/unavailable-code (:error result))))
        (let [lock (acquire! path)] (fs/release-lock! lock)))
      (is (= "" (fs/read-text path))))))

(deftest explicit-empty-single-file-guard-cannot-downgrade-to-default-arity
  (with-store
    (fn [{:keys [path runtime]}]
      (let [result (outcome #(runtime/append! runtime path :record/observed facts/facts {}))]
        (is (= :clio.fs/missing-file-identity (:error result)) (pr-str result))
        (is (= "" (fs/read-text path)))))))
