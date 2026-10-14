(ns clio.infra.ledger-test
  (:require [cljs.test :refer [async deftest is testing]]
            [clio.extern.js.fs :as fs]
            [clio.extern.js.fs-observer :as observer]
            [clio.extern.js.process :as process]
            [clio.extern.js.runtime :as host]
            [clio.infra.event :as event]
            [clio.infra.ledger :as ledger]
            [clio.infra.projection :as projection]
            [clio.infra.runtime :as runtime]
            [clio.law.schema :as schema-law]
            [clio.shape.edn :as edn]
            [clojure.string :as str]
            [promesa.core :as p]))

(def catalog
  {:counter/opened
   (schema-law/event-schema
    :counter/opened
    [:map {:closed true} [:amount :int]])
   :counter/added
   (schema-law/event-schema
    :counter/added
    [:map {:closed true} [:amount :int]])})

(defn apply-counter
  [state event]
  (case (:event/type event)
    :counter/opened (assoc state (:event/stream event)
                           (get-in event [:event/data :amount]))
    :counter/added (update state (:event/stream event) +
                           (get-in event [:event/data :amount]))
    state))

(defn error-code
  [f]
  (try
    (f)
    nil
    (catch :default cause
      (:clio/error (ex-data cause)))))

(defn error-message
  "The message of whatever f throws. Host errors such as ENOENT carry no
   :clio/error, so error-code cannot see them."
  [f]
  (try
    (f)
    nil
    (catch :default cause
      (ex-message cause))))

(deftest ledger-snapshots-read-through-the-lock-and-release-on-read-failure
  (let [directory (str "/tmp/clio-snapshot-" (host/random-uuid))
        file (str directory "/events.edn")
        releases (atom 0)
        release! fs/release-lock!]
    (try
      (fs/ensure-dir! directory)
      (is (= [] (ledger/read-ledger file)) "Singular absent-file inspection remains empty")
      (is (not (fs/exists? file)) "An absent-file inspection does not create storage")
      (ledger/create-ledger! file)
      (let [rt (runtime/open (str directory "/schemas") catalog)
            fact (:event (runtime/append! rt file :counter/opened
                                          {:event/stream "counter:snapshot" :event/seq 1
                                           :event/actor "reader-test" :event/subject "counter:snapshot"
                                           :event/data {:amount 10}}))
            revisions (:schema/revisions rt)]
        (with-redefs [fs/read-text (fn [_] (throw (ex-info "Unlocked path read" {:clio/error :unlocked-read})))]
          (is (= {:events [fact]}
                 (try {:events (:canonical/events (ledger/canonicalize-files revisions [file]))}
                      (catch :default cause {:error (:clio/error (ex-data cause))}))))
          (is (= {:events [fact]}
                 (try {:events (ledger/read-ledger file)}
                      (catch :default cause {:error (:clio/error (ex-data cause))})))))
        (with-redefs [fs/read-locked-text (fn [_] (throw (ex-info "Injected snapshot read failure" {:clio/error :injected-read})))
                      fs/release-lock! (fn [lock] (swap! releases inc) (release! lock))]
          (is (= :injected-read (error-code #(ledger/canonicalize-files revisions [file]))))
          (is (= :injected-read (error-code #(ledger/read-ledger file))))
          (is (= 2 @releases) "Both failed reads release their owning descriptors"))
        (is (= [fact] (:canonical/events (ledger/canonicalize-files revisions [file]))))
        (is (= [fact] (ledger/read-ledger file))))
      (finally (fs/remove-tree! directory)))))

(deftest append-and-projection-are-idempotent
  (let [directory (str "/tmp/clio-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        ledger-file (str directory "/events.edn")
        projection-file (str directory "/counter.edn")]
    (try
      (fs/ensure-dir! directory)
      (ledger/create-ledger! ledger-file)
      (let [rt (runtime/open schema-directory catalog)
            first-write
            (runtime/append!
             rt ledger-file :counter/opened
             {:event/stream "counter:a"
              :event/seq 1
              :event/actor "user:alice"
              :event/subject "counter:a"
              :event/data {:amount 10}})
            first-event (:event first-write)
            second-write
            (runtime/append!
             rt ledger-file :counter/added
             {:event/stream "counter:a"
              :event/seq 2
              :event/causes [(:event/id first-event)]
              :event/actor "user:alice"
              :event/subject "counter:a"
              :event/data {:amount 5}})
            refreshed (runtime/refresh rt)
            retry-result
            (ledger/append-event!
             (:schema/revisions refreshed)
             ledger-file
             (:event second-write))
            options
            {:revisions (:schema/revisions refreshed)
             :ledger-files [ledger-file]
             :output-file projection-file
             :initial {}
             :apply-event apply-counter}
            first-projection (projection/project-files! options)
            second-projection (projection/project-files! options)]
        (testing "schema revisions are persisted automatically"
          (is (= 1 (count (fs/list-files schema-directory)))))
        (testing "retrying an identical append does not duplicate history"
          (is (= :already-present retry-result))
          (is (= 2 (count (ledger/read-ledger ledger-file)))))
        (testing "projection extraction is a pure replay and atomic replacement"
          (is (= first-projection second-projection))
          (is (= {"counter:a" 15}
                 (:projection/state second-projection)))
          (is (fs/exists? projection-file))))
      (finally
        (fs/remove-tree! directory)))))

(deftest ledger-lines-accept-exactly-one-edn-form
  (let [directory (str "/tmp/clio-" (host/random-uuid))
        ledger-file (str directory "/events.edn")]
    (try
      (fs/ensure-dir! directory)
      (fs/write-text! ledger-file "{:a 1} {:b 2}\n")
      (is (= :clio.ledger/invalid-edn
             (error-code #(ledger/read-ledger ledger-file))))
      (finally
        (fs/remove-tree! directory)))))

(deftest append-repairs-a-missing-record-delimiter
  (let [directory (str "/tmp/clio-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        ledger-file (str directory "/events.edn")]
    (try
      (fs/ensure-dir! directory)
      (ledger/create-ledger! ledger-file)
      (let [rt (runtime/open schema-directory catalog)
            first-write
            (runtime/append!
             rt ledger-file :counter/opened
             {:event/stream "counter:a"
              :event/seq 1
              :event/actor "user:alice"
              :event/subject "counter:a"
              :event/data {:amount 1}})
            first-event (:event first-write)
            without-newline
            (subs (fs/read-text ledger-file)
                  0
                  (dec (count (fs/read-text ledger-file))))]
        (fs/write-text! ledger-file without-newline)
        (runtime/append!
         rt ledger-file :counter/added
         {:event/stream "counter:a"
          :event/seq 2
          :event/causes [(:event/id first-event)]
          :event/actor "user:alice"
          :event/subject "counter:a"
          :event/data {:amount 2}})
        (is (= 2 (count (ledger/read-ledger ledger-file)))))
      (finally
        (fs/remove-tree! directory)))))

(deftest concurrent-processes-lock-the-ledger-inode
  (async done
    (let [directory (str "/tmp/clio-" (host/random-uuid))
          schema-directory (str directory "/schemas")
          ledger-file (str directory "/events.edn")
          ledger-alias (str directory "/events-hardlink.edn")
          finish (fn []
                   (fs/remove-tree! directory)
                   (done))]
      (try
        (fs/ensure-dir! directory)
        (ledger/create-ledger! ledger-file)
        (fs/hard-link! ledger-file ledger-alias)
        (let [rt (runtime/open schema-directory catalog)
              revision (:schema/current rt)
              event-a
              (event/make-event
               revision :counter/opened
               {:event/stream "counter:race"
                :event/seq 1
                :event/actor "worker:a"
                :event/subject "counter:race"
                :event/data {:amount 1}})
              event-b
              (event/make-event
               revision :counter/opened
               {:event/stream "counter:race"
                :event/seq 1
                :event/actor "worker:b"
                :event/subject "counter:race"
                :event/data {:amount 2}})
              worker (str (process/cwd) "/test/clio/infra/append_worker.nbb")
              start-at (+ (process/now-ms) 3000)
              command
              (fn [path event]
                {:command "pnpm"
                 :cwd (process/cwd)
                 ;; The workspace nbb; see kernel-lock-test.
                 :args ["exec" "nbb" worker path (pr-str event) (pr-str start-at)]})
              workflow
              (p/let [results
                      (process/run-concurrently!
                       [(command ledger-file event-a)
                        (command ledger-alias event-b)])]
                (let [exit-codes (sort (map :exit-code results))
                      output (str/join "\n" (map :stdout results))]
                  (testing "only one colliding process commits"
                    (is (= [0 1] exit-codes))
                    (is (str/includes? output ":clio.ledger/concurrent-stream-write")))
                  (testing "hard-link aliases share one authoritative inode lock"
                    (is (= 1 (count (ledger/read-ledger ledger-file))))
                    (is (= (ledger/read-ledger ledger-file)
                           (ledger/read-ledger ledger-alias))))
                  (finish)))]
          (p/catch workflow
                   (fn [cause]
                     (is false (str "concurrent append probe failed: " cause))
                     (finish))))
        (catch :default cause
          (is false (str "concurrent append setup failed: " cause))
          (finish))))))

(deftest append-refuses-to-create-a-missing-ledger
  ;; `a+` would create the file, so a misspelled or deleted ledger path would
  ;; quietly become a fresh empty history while the intended ledger stayed
  ;; behind. `create-ledger!` is the only creation path.
  (let [directory (str "/tmp/clio-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        missing-file (str directory "/typo-events.edn")]
    (try
      (fs/ensure-dir! directory)
      (let [rt (runtime/open schema-directory catalog)]
        (testing "the append is refused by name"
          (is (= :clio.ledger/missing-file
                 (error-code
                  #(runtime/append!
                    rt missing-file :counter/opened
                    {:event/stream "counter:a"
                     :event/seq 1
                     :event/actor "user:alice"
                     :event/subject "counter:a"
                     :event/data {:amount 1}})))))
        (testing "and the ledger is still absent afterwards"
          (is (not (fs/exists? missing-file)))))
      (finally
        (fs/remove-tree! directory)))))

(deftest acquiring-a-lock-refuses-to-create-a-missing-ledger
  ;; The same guarantee one layer down: the lock open carries no O_CREAT, so
  ;; even a caller bypassing clio.infra.ledger cannot conjure a ledger.
  (let [directory (str "/tmp/clio-" (host/random-uuid))
        missing-file (str directory "/absent.edn")]
    (try
      (fs/ensure-dir! directory)
      (is (str/includes? (str (error-message #(fs/acquire-lock! missing-file)))
                         "ENOENT")
          "the open fails at the syscall rather than creating the ledger")
      (is (not (fs/exists? missing-file)))
      (finally
        (fs/remove-tree! directory)))))

(deftest a-locked-ledger-refuses-path-based-reads
  ;; A POSIX record lock is released when the process closes ANY descriptor
  ;; for the file, so a path-based read inside a critical section would drop
  ;; the lock silently. The guard turns that into a loud failure.
  (let [directory (str "/tmp/clio-" (host/random-uuid))
        ledger-file (str directory "/events.edn")
        alias (str directory "/events-alias.edn")]
    (try
      (fs/ensure-dir! directory)
      (fs/create-exclusive! ledger-file)
      (fs/hard-link! ledger-file alias)
      (let [lock (fs/acquire-lock! ledger-file)]
        (testing "while the lock is held"
          (is (= :clio.fs/locked-path-read
                 (error-code #(fs/read-text ledger-file))))
          (is (= :clio.fs/locked-path-read
                 (error-code #(fs/read-text alias))))
          (is (= :clio.fs/locked-path-read
                 (error-code #(ledger/read-ledger ledger-file))))
          (is (= :clio.fs/locked-path-read
                 (error-code #(ledger/read-ledger alias))))
          (testing "the lock-owning descriptor still reads"
            (is (= "" (fs/read-locked-text lock)))))
        (fs/release-lock! lock))
      (testing "and the path reads normally once released"
        (is (= "" (fs/read-text ledger-file))))
      (finally
        (fs/remove-tree! directory)))))

(deftest segment-directories-canonicalize-like-one-ledger
  (let [directory (str "/tmp/clio-segments-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        single-file (str directory "/events.edn")
        legacy-file (str directory "/legacy.edn")
        segments (str directory "/segments")]
    (try
      (fs/ensure-dir! directory)
      (ledger/create-ledger! single-file)
      (ledger/create-ledger! legacy-file)
      (let [rt (runtime/open schema-directory catalog)
            opened (:event (runtime/append!
                            rt single-file :counter/opened
                            {:event/stream "counter:a" :event/seq 1
                             :event/actor "user:alice" :event/subject "counter:a"
                             :event/data {:amount 10}}))
            added (:event (runtime/append!
                           rt single-file :counter/added
                           {:event/stream "counter:a" :event/seq 2
                            :event/causes [(:event/id opened)]
                            :event/actor "user:alice" :event/subject "counter:a"
                            :event/data {:amount 5}}))
            revisions (:schema/revisions (runtime/refresh rt))
            whole (ledger/canonicalize-files revisions [single-file])]
        (testing "a missing segment directory is an error, not an empty history"
          (is (= :clio.ledger/missing-directory
                 (error-code #(ledger/segment-paths segments))))
          (is (= :clio.ledger/missing-directory
                 (error-code #(ledger/create-segment! revisions segments added)))))
        (testing "each event becomes its own content-addressed segment"
          (fs/ensure-dir! segments)
          (is (= :appended (ledger/create-segment! revisions segments added)))
          (is (= :appended (ledger/create-segment! revisions segments opened)))
          (is (= :already-present (ledger/create-segment! revisions segments opened)))
          (is (= (sort [(ledger/segment-path segments (:event/id opened))
                        (ledger/segment-path segments (:event/id added))])
                 (ledger/segment-paths segments))))
        (testing "a different event under an existing id collides"
          (is (= :clio.ledger/id-collision
                 (error-code #(ledger/create-segment!
                               revisions segments
                               (assoc-in opened [:event/data :amount] 11))))))
        (testing "segment ids cannot escape the directory"
          (is (= :clio.ledger/invalid-segment-id
                 (error-code #(ledger/segment-path segments "../x")))))
        (testing "segments canonicalize exactly like the single-file ledger"
          (is (= (:canonical/events whole)
                 (:canonical/events (ledger/canonicalize-directory revisions segments []))))
          (is (= (:canonical/events whole)
                 (:canonical/events (runtime/canonicalize-directory rt segments [])))))
        (testing "a frozen legacy ledger is unioned as one more partition"
          (fs/remove-tree! segments)
          (fs/ensure-dir! segments)
          (ledger/append-event! revisions legacy-file opened)
          (ledger/create-segment! revisions segments added)
          (is (= (:canonical/events whole)
                 (:canonical/events
                  (ledger/canonicalize-directory revisions segments [legacy-file])))))
        (testing "runtime append-segment writes a new segment"
          (let [result (runtime/append-segment!
                        rt segments :counter/added
                        {:event/stream "counter:a" :event/seq 3
                         :event/causes [(:event/id added)]
                         :event/actor "user:alice" :event/subject "counter:a"
                         :event/data {:amount 1}})]
            (is (= :appended (:append/result result)))
            (is (= 3 (count (:canonical/events
                             (ledger/canonicalize-directory revisions segments [legacy-file]))))))))
      (finally
        (fs/remove-tree! directory)))))

(deftest segment-directories-fail-closed-and-bind-checked-entries
  (let [directory (str "/tmp/clio-segments-guard-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        segments (str directory "/segments")
        outside (str directory "/outside.edn")]
    (try
      (fs/ensure-dir! directory)
      (fs/ensure-dir! segments)
      (let [rt (runtime/open schema-directory catalog)
            revisions (:schema/revisions (runtime/refresh rt))
            make (fn [stream amount]
                   (event/make-event (:schema/current rt) :counter/opened
                                     {:event/stream stream :event/seq 1
                                      :event/actor "user:alice" :event/subject stream
                                      :event/data {:amount amount}}))
            opened (make "counter:a" 1)
            other (make "counter:b" 2)]
        (testing "an identical retry reflushes under the exclusive lock before acknowledging"
          (is (= :appended (ledger/create-segment! revisions segments opened)))
          (let [syncs (atom 0)
                sync! fs/sync-locked!]
            (with-redefs [fs/sync-locked! (fn [lock] (swap! syncs inc) (sync! lock))]
              (is (= :already-present (ledger/create-segment! revisions segments opened))))
            (is (pos? @syncs) "retry must repeat the durability fence")))
        (testing "the runtime canonicalize-dir path validates segment shape"
          (let [empty-path (ledger/segment-path segments (:event/id other))]
            (ledger/create-ledger! empty-path)
            (is (= :clio.ledger/empty-segment
                   (error-code #(runtime/canonicalize-directory rt segments []))))
            (fs/delete-if-exists! empty-path))
          (let [mismatch (str segments "/0000-mismatch.edn")]
            (ledger/create-ledger! mismatch)
            (ledger/append-event! revisions mismatch other)
            (is (= :clio.ledger/segment-id-mismatch
                   (error-code #(runtime/canonicalize-directory rt segments []))))
            (fs/delete-if-exists! mismatch)))
        (testing "a segment that becomes a symlink after the check is not followed"
          (ledger/create-ledger! outside)
          (let [link (ledger/segment-path segments (:event/id other))]
            (observer/symbolic-link! outside link)
            (with-redefs [fs/symlink? (constantly false)]
              (is (= :clio.ledger/segment-symlink
                     (error-code #(ledger/create-segment! revisions segments other))))
              (is (= "" (fs/read-text outside)) "append must not write outside the directory")
              (ledger/append-event! revisions outside other)
              (is (= :clio.ledger/segment-symlink
                     (error-code #(ledger/canonicalize-directory revisions segments [])))))
            (fs/delete-if-exists! link)))
        (testing "a directory that disappears during enumeration is an error"
          (let [vanishing (str directory "/vanishing")
                exists! fs/exists?
                remove! fs/remove-tree!
                removed? (atom false)]
            (fs/ensure-dir! vanishing)
            ;; The directory is deleted immediately after its existence check.
            (with-redefs [fs/exists? (fn [path]
                                       (let [present (exists! path)]
                                         (when (and present (= path vanishing)
                                                    (compare-and-set! removed? false true))
                                           (remove! vanishing))
                                         present))]
              (is (= :clio.ledger/missing-directory
                     (error-code #(ledger/segment-paths vanishing)))))))
        (testing "canonicalization consumes the validated snapshots, not a reread"
          (let [path (ledger/segment-path segments (:event/id opened))
                reads (atom 0)
                read! fs/read-locked-text]
            (with-redefs [fs/read-locked-text (fn [lock]
                                                (let [text (read! lock)]
                                                  (when (= path (:lock/path lock)) (swap! reads inc))
                                                  text))]
              (is (= [opened] (:canonical/events
                               (ledger/canonicalize-directory revisions segments []))))
              (is (= 1 @reads) "each segment is captured exactly once")))))
      (finally
        (fs/remove-tree! directory)))))

(deftest segment-append-recovery-keeps-segment-admission
  (let [directory (str "/tmp/clio-segments-recovery-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        segments (str directory "/segments")]
    (try
      (fs/ensure-dir! segments)
      (let [rt (runtime/open schema-directory catalog)
            revisions (:schema/revisions (runtime/refresh rt))
            make (fn [stream amount]
                   (event/make-event (:schema/current rt) :counter/opened
                                     {:event/stream stream :event/seq 1
                                      :event/actor "user:alice" :event/subject stream
                                      :event/data {:amount amount}}))
            facts {:event/stream "counter:r" :event/seq 1
                   :event/actor "user:alice" :event/subject "counter:r"
                   :event/data {:amount 3}}
            recovery (try
                       (with-redefs [fs/append-locked-text!
                                     (fn [_ _] (throw (ex-info "Injected append failure" {})))]
                         (runtime/append-segment! rt segments :counter/opened facts))
                       nil
                       (catch :default cause (:clio/append-recovery (ex-data cause))))]
        (testing "segment recovery data names the segment directory"
          (is (string? (:ledger/segment-directory recovery)))
          (is (= :appended (:append/result (runtime/retry-append! rt recovery))))
          (is (= :already-present (:append/result (runtime/retry-append! rt recovery)))))
        (testing "a retried segment never admits a second, different event"
          (let [candidate (make "counter:s" 4)
                stranger (make "counter:t" 5)
                path (ledger/segment-path segments (:event/id candidate))]
            (ledger/create-ledger! path)
            (ledger/append-event! revisions path stranger)
            (is (= :clio.ledger/id-collision
                   (error-code #(runtime/retry-append!
                                 rt {:ledger/path (fs/absolute-path path)
                                     :ledger/segment-directory (fs/absolute-path segments)
                                     :event candidate}))))
            (is (= [stranger] (ledger/read-ledger path)))))
        (testing "segment recovery must name the event's own segment"
          (let [candidate (make "counter:u" 6)]
            (is (= :clio.runtime/invalid-append-recovery
                   (error-code #(runtime/retry-append!
                                 rt {:ledger/path (str (fs/absolute-path segments) "/other.edn")
                                     :ledger/segment-directory (fs/absolute-path segments)
                                     :event candidate})))))))
      (finally
        (fs/remove-tree! directory)))))

(deftest segment-names-accept-exactly-the-admissible-event-ids
  (let [directory (str "/tmp/clio-segments-ids-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        segments (str directory "/segments")
        single-file (str directory "/events.edn")]
    (try
      (fs/ensure-dir! segments)
      (ledger/create-ledger! single-file)
      (let [rt (runtime/open schema-directory catalog)
            revisions (:schema/revisions (runtime/refresh rt))
            valid (event/make-event (:schema/current rt) :counter/opened
                                    {:event/stream "counter:i" :event/seq 1
                                     :event/actor "user:alice" :event/subject "counter:i"
                                     :event/data {:amount 1}})
            ;; 36 characters, so it satisfies the envelope length, but not the identity law.
            odd (assoc valid :event/id "aaaaaaaa_bbbb_4ccc_8ddd_eeeeeeeeeeee")]
        (testing "the segment filename rule is the event identity law"
          (is (string? (ledger/segment-path segments (:event/id valid))))
          (is (string? (ledger/segment-path segments (str/upper-case (:event/id valid)))))
          (is (= :clio.ledger/invalid-segment-id
                 (error-code #(ledger/segment-path segments "not-a-uuid")))))
        (testing "an id the ordinary ledger refuses is refused the same way as a segment"
          (is (= :clio.schema/invalid-identity
                 (error-code #(ledger/append-event! revisions single-file odd))))
          (is (= :clio.schema/invalid-identity
                 (error-code #(ledger/create-segment! revisions segments odd)))))
        (testing "an admissible id is admitted by both representations"
          (is (= :appended (ledger/append-event! revisions single-file valid)))
          (is (= :appended (ledger/create-segment! revisions segments valid)))))
      (finally
        (fs/remove-tree! directory)))))

(deftest segment-publish-recovery-and-entry-guards
  (let [directory (str "/tmp/clio-segments-p2-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        segments (str directory "/segments")
        outside (str directory "/outside.edn")]
    (try
      (fs/ensure-dir! segments)
      (let [rt (runtime/open schema-directory catalog)
            revisions (:schema/revisions (runtime/refresh rt))
            make (fn [stream amount]
                   (event/make-event (:schema/current rt) :counter/opened
                                     {:event/stream stream :event/seq 1
                                      :event/actor "user:alice" :event/subject stream
                                      :event/data {:amount amount}}))
            opened (make "counter:a" 1)
            other (make "counter:b" 2)
            facts {:event/stream "counter:m" :event/seq 1
                   :event/actor "user:alice" :event/subject "counter:m"
                   :event/data {:amount 9}}]
        (testing "a new segment is published only after the event is written"
          (let [link! fs/hard-link!
                saw-complete (atom false)]
            (with-redefs [fs/hard-link! (fn [from to]
                                         (is (fs/exists? from) "temp must exist under the writer lock")
                                         (is (str/includes? from ".creating-")
                                             "publish must use a hidden creating temp")
                                         (is (not (fs/exists? to))
                                             "final name must be absent until exclusive claim")
                                         (is (not (some #(= % to) (ledger/segment-paths segments))))
                                         (reset! saw-complete true)
                                         (link! from to))]
              (is (= :appended (ledger/create-segment! revisions segments opened)))
              (is @saw-complete)
              (is (= [opened] (:canonical/events
                               (ledger/canonicalize-directory revisions segments [])))))))
        (testing "segment setup failures preserve append recovery"
          (let [missing (str directory "/missing-segments")
                cause (try
                        (runtime/append-segment! rt missing :counter/opened facts)
                        nil
                        (catch :default e e))
                recovery (:clio/append-recovery (ex-data cause))]
            (is (= :clio.ledger/missing-directory (:clio/error (ex-data cause))))
            (is (map? recovery))
            (is (some? (:event/id (:event recovery))))
            (is (= missing (:ledger/segment-directory recovery)))))
        (testing "a FIFO segment entry fails closed without hanging"
          (let [fifo (ledger/segment-path segments (:event/id other))]
            (observer/mkfifo! fifo)
            (is (= :clio.fs/not-regular-file
                   (error-code #(ledger/canonicalize-directory revisions segments []))))
            (fs/delete-if-exists! fifo)))
        (testing "a hard-linked segment entry is refused"
          (ledger/create-ledger! outside)
          (let [linked (ledger/segment-path segments (:event/id other))]
            (fs/hard-link! outside linked)
            (is (= :clio.ledger/segment-hardlink
                   (error-code #(ledger/create-segment! revisions segments other))))
            (is (= "" (fs/read-text outside))
                "append must not write through the hard link")
            (is (= :clio.ledger/segment-hardlink
                   (error-code #(ledger/canonicalize-directory revisions segments []))))
            (fs/delete-if-exists! linked))))
      (finally
        (fs/remove-tree! directory)))))

(defn- creating-entries
  [segments]
  (filterv #(str/includes? % ".creating-") (fs/list-directory segments)))

(defn- interrupt-publish!
  "Leave the state of a publisher stopped between its exclusive hard link and
   the temp delete: the published segment plus a hidden temp alias of its inode."
  [segments event]
  (let [path (ledger/segment-path segments (:event/id event))]
    (fs/hard-link! path (str segments "/." (:event/id event) ".creating-" (host/random-uuid)))
    path))

(deftest interrupted-publish-aliases-are-repaired-under-the-segment-lock
  (let [directory (str "/tmp/clio-segments-stale-" (host/random-uuid))
        schema-directory (str directory "/schemas")
        segments (str directory "/segments")]
    (try
      (fs/ensure-dir! segments)
      (let [rt (runtime/open schema-directory catalog)
            revisions (:schema/revisions (runtime/refresh rt))
            make (fn [stream amount]
                   (event/make-event (:schema/current rt) :counter/opened
                                     {:event/stream stream :event/seq 1
                                      :event/actor "user:alice" :event/subject stream
                                      :event/data {:amount amount}}))
            opened (make "counter:a" 1)
            other (make "counter:b" 2)]
        (testing "publication drops the temp alias before the directory fence"
          (let [path (ledger/segment-path segments (:event/id opened))
                sync! fs/sync-directory!
                overlaps (atom [])]
            (with-redefs [fs/sync-directory! (fn [dir]
                                               (when (and (fs/exists? path)
                                                          (seq (creating-entries segments)))
                                                 (swap! overlaps conj dir))
                                               (sync! dir))]
              (is (= :appended (ledger/create-segment! revisions segments opened))))
            (is (= [] @overlaps) "no directory fence persists both names of one inode")
            (is (= 1 (observer/link-count path)))))
        (testing "canonicalization repairs a stale temp alias of the same inode"
          (let [path (interrupt-publish! segments opened)]
            (is (= 2 (observer/link-count path)))
            (is (= [opened] (:canonical/events
                             (ledger/canonicalize-directory revisions segments []))))
            (is (= [] (creating-entries segments)))
            (is (= 1 (observer/link-count path)))))
        (testing "an exact retry repairs the alias and is :already-present"
          (let [path (interrupt-publish! segments opened)]
            (is (= :already-present (ledger/create-segment! revisions segments opened)))
            (is (= [] (creating-entries segments)))
            (is (= 1 (observer/link-count path)))))
        (testing "runtime retry-append! finishes the preserved recovery"
          (is (= :appended (ledger/create-segment! revisions segments other)))
          (let [path (interrupt-publish! segments other)]
            (is (= :already-present
                   (:append/result
                    (runtime/retry-append! rt {:ledger/path (fs/absolute-path path)
                                               :ledger/segment-directory (fs/absolute-path segments)
                                               :event other}))))
            (is (= [] (creating-entries segments)))
            (is (= 1 (observer/link-count path)))))
        (testing "only this segment's own temp aliases of its own inode are removed"
          (let [path (ledger/segment-path segments (:event/id other))
                unrelated (str segments "/." (:event/id other) ".creating-" (host/random-uuid))
                misnamed (str segments "/." (:event/id opened) ".creating-" (host/random-uuid))]
            (ledger/create-ledger! unrelated)
            (fs/hard-link! path misnamed)
            (is (= :clio.ledger/segment-hardlink
                   (error-code #(ledger/canonicalize-directory revisions segments []))))
            (is (fs/exists? unrelated) "a different inode under this segment's temp prefix is kept")
            (is (fs/exists? misnamed) "another segment's temp name is never treated as this segment's")
            (is (= 2 (observer/link-count path)))
            (fs/delete-if-exists! misnamed)
            (fs/delete-if-exists! unrelated)
            (is (= [opened other]
                   (sort-by #(:amount (:event/data %))
                            (:canonical/events (ledger/canonicalize-directory revisions segments []))))))))
      (finally
        (fs/remove-tree! directory)))))

(deftest concurrent-processes-publish-one-segment
  (async done
    (let [directory (str "/tmp/clio-segments-race-" (host/random-uuid))
          schema-directory (str directory "/schemas")
          finish (fn []
                   (fs/remove-tree! directory)
                   (done))]
      (try
        (fs/ensure-dir! directory)
        (let [rt (runtime/open schema-directory catalog)
              revision (:schema/current rt)
              make (fn [actor amount]
                     (event/make-event
                      revision :counter/opened
                      {:event/stream "counter:race" :event/seq 1
                       :event/actor actor :event/subject "counter:race"
                       :event/data {:amount amount}}))
              event-a (make "worker:a" 1)
              ;; Different data under the same id.
              event-b (assoc (make "worker:b" 2) :event/id (:event/id event-a))
              worker (str (process/cwd) "/test/clio/infra/segment_worker.nbb")
              race! (fn [segments events]
                      (fs/ensure-dir! segments)
                      (let [start-at (+ (process/now-ms) 3000)]
                        (p/let [results (process/run-concurrently!
                                         (mapv (fn [evt]
                                                 {:command "pnpm"
                                                  :cwd (process/cwd)
                                                  ;; The workspace nbb; see kernel-lock-test.
                                                  :args ["exec" "nbb" worker segments
                                                         (pr-str evt) (pr-str start-at)]})
                                               events))]
                          (mapv (fn [{:keys [stdout]}]
                                  (let [{:keys [result error]}
                                        (some-> (re-find #"\{:(?:result|error)[^\n]*\}" stdout)
                                                edn/read-one)]
                                    (or result error stdout)))
                                results))))
              settled? (fn [segments event]
                         (let [path (ledger/segment-path segments (:event/id event))]
                           (is (= [event] (ledger/read-ledger path)) "the segment holds one event")
                           (is (= 1 (observer/link-count path)))
                           (is (= [] (creating-entries segments)) "no publish temp remains")))
              workflow
              (p/let [same (race! (str directory "/same") [event-a event-a])
                      _ (testing "two processes publishing the same event"
                          (is (= #{:appended :already-present} (set same)) (pr-str same))
                          (settled? (str directory "/same") event-a))
                      clash (race! (str directory "/clash") [event-a event-b])]
                (testing "two processes publishing different data under one id"
                  (is (= #{:appended :clio.ledger/id-collision} (set clash)) (pr-str clash))
                  (let [winner (if (= :appended (first clash)) event-a event-b)]
                    (settled? (str directory "/clash") winner)))
                (finish))]
          (p/catch workflow
                   (fn [cause]
                     (is false (str "concurrent segment probe failed: " cause))
                     (finish))))
        (catch :default cause
          (is false (str "concurrent segment setup failed: " cause))
          (finish))))))
