(ns clio.infra.directory-binding-test
  (:require #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer [deftest is]])
            #?(:clj [clio.extern.jvm.fs :as fs]
               :cljs [clio.extern.js.fs :as fs])
            [clio.infra.event :as event]
            [clio.infra.host-fixture :as fixture]
            [clio.infra.ledger :as ledger]
            [clio.infra.runtime :as runtime]
            [clojure.string :as str]))

(defn- outcome [f]
  (try (f)
       (catch #?(:clj Exception :cljs :default) cause
         {:error (ex-data cause) :message (ex-message cause)})))

(defn- with-stores [f]
  (let [root (str "/tmp/clio-directory-binding-" (random-uuid))
        store (str root "/store")
        other (str root "/other")
        moved (str root "/moved")
        swapped? (atom false)
        swap! (fn []
                (fs/rename! store moved)
                (fs/rename! other store)
                (reset! swapped? true))
        restore! (fn []
                   (when @swapped?
                     (fs/rename! store other)
                     (fs/rename! moved store)
                     (reset! swapped? false)))]
    (try
      (fs/ensure-dir! store)
      (fs/ensure-dir! other)
      (let [rt (runtime/open (str root "/schemas") fixture/catalog)
            revisions (:schema/revisions rt)
            a (event/make-event (:schema/current rt) :record/observed fixture/facts)
            b (assoc-in a [:event/data :amount] 99)
            extra (event/make-event (:schema/current rt) :record/observed
                                    (assoc fixture/facts :event/stream "record:b"))]
        (ledger/create-segment! revisions store a)
        (ledger/create-segment! revisions other b)
        (ledger/create-segment! revisions other extra)
        (f {:store store :other other :a a :revisions revisions
            :swap! swap! :restore! restore!}))
      (finally
        (restore!)
        (fs/remove-tree! root)))))

(deftest directory-aba-around-a-segment-open-cannot-substitute-an-event
  (with-stores
    (fn [{:keys [store a revisions swap! restore!]}]
      (let [acquire! fs/acquire-entry-read-lock!
            owns? fs/locked-entry-owns-path?
            armed? (atom true)]
        (with-redefs [fs/acquire-entry-read-lock!
                      (fn [path options]
                        (when (compare-and-set! armed? true false) (swap!))
                        (acquire! path options))
                      fs/locked-entry-owns-path?
                      (fn [lock path]
                        (let [result (owns? lock path)]
                          (restore!)
                          result))]
          (is (= [a] (outcome #(:canonical/events
                                (ledger/canonicalize-directory revisions store []))))
              "the same-named valid event in a temporarily substituted store is not read"))
        (is (false? @armed?) "the attack reached the actual segment open")))))

(deftest directory-aba-around-enumeration-cannot-add-another-stores-segment
  (with-stores
    (fn [{:keys [store a revisions swap! restore!]}]
      (let [list! fs/list-directory
            armed? (atom true)]
        (with-redefs [fs/list-directory
                      (fn [path]
                        (if (compare-and-set! armed? true false)
                          (do (swap!)
                              (try (list! path) (finally (restore!))))
                          (list! path)))]
          (is (= [a] (outcome #(:canonical/events
                                (ledger/canonicalize-directory revisions store []))))
              "enumeration belongs to the captured directory, including its entry set"))
        (is (false? @armed?))))))

(deftest directory-aba-on-the-existing-entry-path-cannot-fence-a-different-store
  (with-stores
    (fn [{:keys [store a swap! restore!]}]
      (let [path (ledger/segment-path store (:event/id a))
            name (last (str/split path #"/"))
            before (fs/read-text path)
            exists? fs/exists?
            force! fs/sync-locked!
            release! fs/release-lock!
            forced (atom [])
            armed? (atom true)]
        (with-redefs [fs/exists?
                      (fn [p]
                        (let [result (exists? p)]
                          (when (and result (str/ends-with? p name)
                                     (compare-and-set! armed? true false))
                            (swap!))
                          result))
                      fs/sync-locked!
                      (fn [lock]
                        (clojure.core/swap! forced conj (fs/read-locked-text lock))
                        (force! lock))
                      fs/release-lock!
                      (fn [lock]
                        (release! lock)
                        (restore!))]
          (is (= path (outcome #(ledger/ensure-segment-file! store (:event/id a))))))
        (is (= [before] @forced) "durability acknowledgment fences the intended inode")
        (is (false? @armed?))))))

(deftest directory-aba-around-temp-creation-cannot-leak-a-write-to-another-store
  (with-stores
    (fn [{:keys [store other a revisions swap! restore!]}]
      (let [new-event (assoc a :event/id (str (random-uuid)) :event/stream "record:new")
            original-entries (sort (fs/list-directory other))
            create! fs/create-exclusive!
            armed? (atom true)]
        (with-redefs [fs/create-exclusive!
                      (fn [path]
                        (if (and (str/includes? path ".creating-")
                                 (compare-and-set! armed? true false))
                          (do (swap!)
                              (try (create! path) (finally (restore!))))
                          (create! path)))]
          (is (= :appended (outcome #(ledger/create-segment! revisions store new-event)))))
        (is (= [new-event] (ledger/read-ledger (ledger/segment-path store (:event/id new-event)))))
        (is (= original-entries (sort (fs/list-directory other)))
            "the substituted store receives neither an event nor an orphan temporary")
        (is (false? @armed?))))))
