(ns clio.infra.jvm-ledger-test
  (:require [clio.extern.jvm.fs :as fs]
            [clio.extern.jvm.runtime :as host]
            [clio.extern.jvm.test-support :as support]
            [clio.infra.event :as event]
            [clio.infra.host-fixture :as fixture]
            [clio.infra.ledger :as ledger]
            [clio.infra.projection :as projection]
            [clio.infra.runtime :as runtime]
            [clio.infra.schema-store :as schema-store]
            [clio.shape.canonical :as canonical]
            [clio.shape.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]))

(defn error-code [f]
  (try (f) nil (catch Exception cause (:clio/error (ex-data cause)))))

(defn with-ledger [f]
  (let [directory (str "/tmp/clio-jvm-" (host/random-uuid))
        path (str directory "/events.edn")
        schemas (str directory "/schemas")]
    (try
      (fs/ensure-dir! directory)
      (ledger/create-ledger! path)
      (f {:directory directory :path path :schemas schemas
          :runtime (runtime/open schemas fixture/catalog)})
      (finally (fs/remove-tree! directory)))))

(defn reduce-amount [state evt]
  (+ state (get-in evt [:event/data :amount])))

(deftest jvm-entry-link-count-retains-the-open-descriptor
  (with-ledger
    (fn [{:keys [directory]}]
      (let [entry (str directory "/entry.edn")]
        (ledger/create-ledger! entry)
        (doseq [acquire! [fs/acquire-entry-lock!
                         fs/acquire-entry-read-lock!
                         fs/try-acquire-entry-lock!]]
          (let [token (acquire! entry)]
            (try
              ;; Discovery can become unavailable after acquisition; the
              ;; retained channel still owns the same descriptor and inode.
              (with-redefs [fs/channel-fd-int
                            (fn [_]
                              (throw (ex-info "Descriptor rediscovery unavailable"
                                              {:clio/error :unexpected-descriptor-scan})))]
                (dotimes [_ 2]
                  (is (= 1 (try (fs/locked-link-count token)
                                (catch Exception cause
                                  (:clio/error (ex-data cause))))))))
              (finally (fs/release-lock! token)))))))))

(deftest jvm-canonical-snapshots-release-the-owning-channel-on-read-failure
  (with-ledger
    (fn [{:keys [path runtime]}]
      (let [fact (:event (runtime/append! runtime path :record/observed fixture/facts))
            revisions (:schema/revisions runtime)
            releases (atom 0)
            release! fs/release-lock!]
        (with-redefs [fs/read-text (fn [_] (throw (ex-info "Unlocked path read" {:clio/error :unlocked-read})))]
          (is (= {:events [fact]}
                 (try {:events (:canonical/events (ledger/canonicalize-files revisions [path]))}
                      (catch Exception cause {:error (:clio/error (ex-data cause))})))))
        (with-redefs [fs/read-locked-text (fn [_] (throw (ex-info "Injected snapshot read failure" {:clio/error :injected-read})))
                      fs/release-lock! (fn [lock] (swap! releases inc) (release! lock))]
          (is (= :injected-read (error-code #(ledger/canonicalize-files revisions [path]))))
          (is (= 1 @releases)))
        (is (= [fact] (:canonical/events (ledger/canonicalize-files revisions [path]))))))))

(deftest jvm-restart-rebuilds-typed-events-and-projections
  (with-ledger
    (fn [{:keys [directory path schemas runtime]}]
      (let [{evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            reopened (runtime/open schemas fixture/catalog)
            options {:revisions (:schema/revisions reopened) :ledger-files [path]
                     :output-file (str directory "/projection.edn")
                     :initial 0 :apply-event reduce-amount}]
        (is (= :already-present (ledger/append-event! (:schema/revisions reopened) path evt)))
        (is (= [evt] (ledger/read-ledger path)))
        (is (= fixture/payload (:event/data (first (ledger/read-ledger path)))))
        (is (re-matches #".*\.\d{3}Z" (:event/at evt)))
        (is (= (projection/project-files! options) (projection/project-files! options)))
        (is (= 7 (:projection/state (edn/read-one (fs/read-text (:output-file options))))))))))

(deftest jvm-preserves-admission-and-fail-closed-history
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [{evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions runtime)
            missing (str directory "/missing.edn")]
        (is (= :clio.ledger/id-collision
               (error-code #(ledger/append-event! revisions path
                                                  (assoc-in evt [:event/data :amount] 8)))))
        (is (= :clio.ledger/concurrent-stream-write
               (error-code #(ledger/append-event! revisions path
                                                  (assoc evt :event/id (host/random-uuid))))))
        (is (= :clio.ledger/missing-file
               (error-code #(ledger/append-event! revisions missing evt))))
        (is (= :clio.ledger/missing-file
               (error-code #(ledger/canonicalize-files revisions [path missing]))))
        (is (not (fs/exists? missing)))
        (is (= :clio.fs/not-regular-file (error-code #(fs/acquire-lock! directory))))
        (fs/write-text! path "{:first 1} {:second 2}\n")
        (is (= :clio.ledger/invalid-edn (error-code #(ledger/read-ledger path))))))))

(deftest jvm-rejects-corrupt-schema-snapshots
  (with-ledger
    (fn [{:keys [schemas runtime]}]
      (let [root (get-in runtime [:schema/current :schema/root])
            path (schema-store/revision-path schemas root)]
        (fs/write-text! path (pr-str {:schema/root root :schema/catalog {}}))
        (is (= :clio.schema-store/corrupt-snapshot
               (error-code #(schema-store/load-revisions schemas))))))))

(deftest jvm-path-guard-protects-hard-link-aliases
  (with-ledger
    (fn [{:keys [directory path]}]
      (let [alias (str directory "/alias.edn")]
        (fs/hard-link! path alias)
        (let [lock (fs/acquire-lock! path)]
          (try
            (is (= "" (fs/read-locked-text lock)))
            (is (= :clio.fs/locked-path-read (error-code #(fs/read-text path))))
            (is (= :clio.fs/locked-path-read (error-code #(fs/read-text alias))))
            (is (= :clio.fs/locked-path-read (error-code #(fs/acquire-lock! alias))))
            (finally (fs/release-lock! lock))))
        (is (= "" (fs/read-text alias)))))))

(deftest malformed-host-values-are-not-canonical-edn
  (is (= :clio.canonical/invalid-instant
         (error-code #(canonical/canonical-edn (support/invalid-instant)))))
  (is (= :clio.canonical/unsupported-value
         (error-code #(canonical/canonical-edn (support/non-edn-instant)))))
  (is (= :clio.canonical/unsupported-value
         (error-code #(canonical/canonical-edn (support/arbitrary-object))))))

(deftest schema-rename-forces-the-directory-before-acknowledgement
  (with-ledger
    (fn [{:keys [directory]}]
      (let [from (str directory "/pending.edn")
            to (str directory "/committed.edn")
            synchronize! fs/sync-directory!
            observations (atom [])]
        (fs/write-text! from "{:schema :new}")
        (with-redefs [fs/sync-directory!
                      (fn [path]
                        (swap! observations conj [(fs/exists? from) (fs/exists? to)])
                        (synchronize! path))]
          (is (= to (fs/rename! from to))))
        (is (= [[true false] [false true]] @observations))
        (is (= "{:schema :new}" (fs/read-text to)))))))

(deftest unsupported-directory-sync-refuses-before-changing-schema-paths
  (with-ledger
    (fn [{:keys [directory]}]
      (let [from (str directory "/pending.edn") to (str directory "/committed.edn")]
        (fs/write-text! from "{:schema :new}")
        (with-redefs [fs/sync-directory!
                      (fn [_] (throw (ex-info "unsupported directory force"
                                              {:clio/error :clio.fs/directory-sync-unavailable})))]
          (is (= :clio.fs/directory-sync-unavailable (error-code #(fs/rename! from to)))))
        (is (fs/exists? from))
        (is (not (fs/exists? to)))))))

(deftest node-and-jvm-share-schemas-edn-tags-and-replay
  (with-ledger
    (fn [{:keys [directory path schemas runtime]}]
      (let [evt (:event (runtime/append! runtime path :record/observed fixture/facts))
            output (str directory "/peer.out")
            peer (support/start! ["nbb" "-cp" "test" "test/clio/infra/jvm_peer.nbb"
                                  "read" schemas path] output)]
        (try
          (is (= 0 (support/finish! peer)) (fs/read-text output))
          (let [result (edn/read-one (str/trim (fs/read-text output)))]
            (is (= (get-in runtime [:schema/current :schema/root]) (:root result)))
            (is (= [evt] (:events result)))
            (is (= (projection/build
                    (ledger/canonicalize-files (:schema/revisions runtime) [path])
                    0 reduce-amount)
                   (:projection result))))
          (finally (support/stop! peer)))))))

(deftest jvm-reopens-node-created-durable-schema-and-ledger
  (let [directory (str "/tmp/clio-node-first-" (host/random-uuid))
        child (str directory "/node-created")
        output (str directory "/peer.out")]
    (try
      (fs/ensure-dir! directory)
      (let [peer (support/start! ["nbb" "-cp" "test" "test/clio/infra/durability_peer.nbb" child] output)]
        (try
          (is (= 0 (support/finish! peer)) (fs/read-text output))
          (let [result (edn/read-one (str/trim (fs/read-text output)))
                reopened (runtime/open (str child "/nested/schemas") fixture/catalog)
                path (str child "/events.edn")
                events (:canonical/events (ledger/canonicalize-files (:schema/revisions reopened) [path]))]
            (is (= :appended (:append/result result)))
            (is (= [(:event result)] events))
            (is (= fixture/payload (:event/data (first events))))
            (is (= (get-in reopened [:schema/current :schema/root])
                   (get-in result [:event :event/schema :schema/root]))))
          (finally (support/stop! peer))))
      (finally (fs/remove-tree! directory)))))

(deftest node-replays-both-admitted-calendar-boundaries-from-jvm
  (doseq [millis [-12219292800000 253402300799999]]
    (with-ledger
      (fn [{:keys [directory path schemas runtime]}]
        (let [facts (assoc-in fixture/facts [:event/data :observed/at] (support/instant-at millis))
              evt (:event (runtime/append! runtime path :record/observed facts))
              output (str directory "/boundary-peer.out")
              peer (support/start! ["nbb" "-cp" "test" "test/clio/infra/jvm_peer.nbb"
                                    "read" schemas path] output)]
          (try
            (is (= 0 (support/finish! peer)) (fs/read-text output))
            (let [result (edn/read-one (str/trim (fs/read-text output)))]
              (is (= [evt] (:events result)))
              (is (= millis (inst-ms (get-in result [:events 0 :event/data :observed/at])))))
            (finally (support/stop! peer))))))))

(deftest node-contender-blocks-on-jvm-inode-lock
  (with-ledger
    (fn [{:keys [directory path schemas runtime]}]
      (let [revision (:schema/current runtime)
            winner (event/make-event revision :record/observed fixture/facts)
            contender (event/make-event revision :record/observed fixture/facts)
            alias (str directory "/alias.edn")
            ready (str directory "/ready")
            output (str directory "/peer.out")
            _ (fs/hard-link! path alias)
            lock (fs/acquire-lock! path)
            peer (support/start! ["nbb" "-cp" "test" "test/clio/infra/jvm_peer.nbb"
                                  "append" schemas alias ready (pr-str contender)] output)]
        (try
          (try
            (support/wait-for-path! ready)
            (is (not (support/exited? peer 250)) "Node must wait for JVM's fcntl inode lock")
            (fs/append-locked-text! lock (str (pr-str winner) "\n"))
            (finally (fs/release-lock! lock)))
          (testing "the loser observes the committed event after acquiring the lock"
            (is (= 1 (support/finish! peer)))
            (is (str/includes? (fs/read-text output) ":clio.ledger/concurrent-stream-write"))
            (is (= [winner] (ledger/read-ledger path))))
          (finally (support/stop! peer)))))))

(deftest jvm-contender-blocks-on-node-inode-lock
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [ready (str directory "/ready")
            output (str directory "/peer.out")
            peer (support/start! ["nbb" "test/clio/infra/lock_holder.nbb"
                                  path ready "800"] output)]
        (try
          (support/wait-for-path! ready)
          (let [started (support/now-ms)]
            (is (= :appended
                   (:append/result (runtime/append! runtime path :record/observed fixture/facts))))
            (is (>= (- (support/now-ms) started) 400)
                "JVM must wait for Node's fcntl inode lock"))
          (is (= 0 (support/finish! peer)) (fs/read-text output))
          (finally (support/stop! peer)))))))

(deftest jvm-single-reader-waits-for-node-to-complete-its-locked-append
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [fact (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            partial-ready (str directory "/partial-ready")
            reader-ready (str directory "/reader-ready")
            output (str directory "/writer.out")
            peer (support/start! ["nbb" "-cp" "test" "test/clio/infra/partial_writer.nbb"
                                  path partial-ready reader-ready (pr-str fact)] output)
            read-text fs/read-text
            acquire-lock! fs/acquire-lock!
            acquire-read-lock! fs/acquire-read-lock!
            signal! (fn [candidate]
                      (when (= path candidate)
                        (fs/write-text! reader-ready "reading")))]
        (try
          (support/wait-for-path! partial-ready)
          (let [started (support/now-ms)
                result (with-redefs [fs/read-text (fn [candidate]
                                                   (signal! candidate) (read-text candidate))
                                    fs/acquire-lock! (fn [candidate]
                                                       (signal! candidate) (acquire-lock! candidate))
                                    fs/acquire-read-lock! (fn [candidate]
                                                            (signal! candidate) (acquire-read-lock! candidate))]
                         (try {:events (ledger/read-ledger path)}
                              (catch Exception cause {:error (:clio/error (ex-data cause))})))]
            (is (= {:events [fact]} result)
                "The public singular reader must not parse a writer's incomplete final line")
            (is (>= (- (support/now-ms) started) 200)
                "Reading waits until the peer completes its append and releases the inode"))
          (is (= 0 (support/finish! peer)) (fs/read-text output))
          (is (= [fact] (ledger/read-ledger path)))
          (finally (support/stop! peer)))))))

(deftest node-single-reader-waits-for-jvm-to-complete-its-locked-append
  (with-ledger
    (fn [{:keys [directory path schemas runtime]}]
      (let [fact (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            text (pr-str fact)
            split (quot (count text) 2)
            ready (str directory "/reader-ready")
            output (str directory "/reader.out")
            lock (fs/acquire-lock! path)
            peer (atom nil)]
        (try
          (try
            (fs/append-locked-text! lock (subs text 0 split))
            (reset! peer (support/start! ["nbb" "-cp" "test" "test/clio/infra/jvm_peer.nbb"
                                         "read-single" schemas path ready] output))
            (support/wait-for-path! ready)
            (is (not (support/exited? @peer 250))
                "Node's singular reader must wait on the JVM's actual POSIX inode lock")
            (fs/append-locked-text! lock (str (subs text split) "\n"))
            (finally (fs/release-lock! lock)))
          (is (= 0 (support/finish! @peer)) (fs/read-text output))
          (is (= {:events [fact]} (edn/read-one (str/trim (fs/read-text output)))))
          (is (= [fact] (ledger/read-ledger path)))
          (finally (when @peer (support/stop! @peer))))))))

(deftest both-hosts-replay-a-read-only-ledger-without-changing-its-bytes
  (with-ledger
    (fn [{:keys [directory path schemas runtime]}]
      (let [fact (:event (runtime/append! runtime path :record/observed fixture/facts))
            before (fs/read-text path)
            output (str directory "/read-only-peer.out")
            ready (str directory "/read-only-peer-ready")]
        (support/permissions! path "r--r--r--")
        (try
          (is (= {:events [fact]}
                 (try {:events (ledger/read-ledger path)}
                      (catch Exception cause {:error (ex-message cause)})))
              "Inspection needs read permission, including while using an advisory lock")
          (let [peer (support/start! ["nbb" "-cp" "test" "test/clio/infra/jvm_peer.nbb"
                                      "read-single" schemas path ready] output)]
            (try
              (is (= 0 (support/finish! peer)) (fs/read-text output))
              (is (= {:events [fact]} (edn/read-one (str/trim (fs/read-text output)))))
              (finally (support/stop! peer))))
          (is (= before (fs/read-text path)) "Replay never changes read-only history")
          (finally (support/permissions! path "rw-r--r--")))))))

(deftest jvm-shared-descriptor-refuses-writes-and-reentry-even-with-write-permission
  (with-ledger
    (fn [{:keys [directory path]}]
      (let [alias (str directory "/read-alias.edn")]
        (fs/write-text! path "retained")
        (fs/hard-link! path alias)
        (let [lock (fs/acquire-read-lock! path)]
          (try
            (is (= "retained" (fs/read-locked-text lock)))
            (is (thrown? java.nio.channels.NonWritableChannelException
                         (fs/append-locked-text! lock "corruption")))
            (doseq [candidate [path alias]
                    acquire! [fs/acquire-read-lock! fs/acquire-lock!]]
              (is (= :clio.fs/locked-path-read (error-code #(acquire! candidate)))))
            (finally (fs/release-lock! lock))))
        (is (= "retained" (fs/read-text path)))
        (let [writer (fs/acquire-lock! path)]
          (try (fs/append-locked-text! writer " after release")
               (finally (fs/release-lock! writer))))
        (is (= "retained after release" (fs/read-text path)))))))

(deftest jvm-segment-directory-matches-single-file
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))]
        (is (= :clio.ledger/missing-directory (error-code #(ledger/segment-paths segments))))
        (is (= :clio.ledger/missing-directory (error-code #(ledger/create-segment! revisions segments evt))))
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (is (= :already-present (ledger/create-segment! revisions segments evt)))
        (is (= [(ledger/segment-path segments (:event/id evt))] (ledger/segment-paths segments)))
        (is (= (:canonical/events (ledger/canonicalize-files revisions [path]))
               (:canonical/events (ledger/canonicalize-directory revisions segments []))
               (:canonical/events (ledger/canonicalize-directory revisions segments [path]))))))))

(deftest jvm-segment-entries-are-bound-validated-and-reflushed
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            outside (str directory "/outside.edn")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            other (event/make-event (:schema/current runtime) :record/observed
                                    (assoc fixture/facts :event/stream "record:other"
                                           :event/subject "record:other"))]
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (testing "an identical retry repeats the durability fence"
          (let [syncs (atom 0) sync! fs/sync-locked!]
            (with-redefs [fs/sync-locked! (fn [lock] (swap! syncs inc) (sync! lock))]
              (is (= :already-present (ledger/create-segment! revisions segments evt))))
            (is (pos? @syncs))))
        (testing "runtime canonicalize-directory validates segment shape"
          (let [empty-path (ledger/segment-path segments (:event/id other))]
            (ledger/create-ledger! empty-path)
            (is (= :clio.ledger/empty-segment
                   (error-code #(runtime/canonicalize-directory runtime segments []))))
            (fs/delete-if-exists! empty-path)))
        (testing "a symlink swapped in after the check is refused, not followed"
          (ledger/create-ledger! outside)
          (let [link (ledger/segment-path segments (:event/id other))]
            (java.nio.file.Files/createSymbolicLink
             (java.nio.file.Paths/get link (make-array String 0))
             (java.nio.file.Paths/get outside (make-array String 0))
             (make-array java.nio.file.attribute.FileAttribute 0))
            (with-redefs [fs/symlink? (constantly false)]
              (is (= :clio.ledger/segment-symlink
                     (error-code #(ledger/create-segment! revisions segments other))))
              (is (= "" (fs/read-text outside)))
              (is (= :clio.ledger/segment-symlink
                     (error-code #(ledger/canonicalize-directory revisions segments [])))))
            (fs/delete-if-exists! link)))
        (is (= [evt] (:canonical/events (runtime/canonicalize-directory runtime segments []))))))))

(deftest jvm-locks-need-no-consumer-module-opens
  ;; A :local/root consumer inherits none of Clio's aliases, so ordinary and
  ;; segment-entry locks must work on a JVM started without --add-opens.
  (is (not-any? #(str/includes? % "add-opens")
                (.getInputArguments (java.lang.management.ManagementFactory/getRuntimeMXBean))))
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))]
        (fs/release-lock! (fs/acquire-read-lock! path))
        (fs/release-lock! (fs/acquire-lock! path))
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (is (= [evt] (:canonical/events
                      (ledger/canonicalize-directory revisions segments []))))))))

(defn- mkfifo! [path mode]
  (let [process (.start (ProcessBuilder. ^java.util.List ["mkfifo" "-m" mode path]))]
    (when-not (zero? (.waitFor process))
      (throw (ex-info "mkfifo failed" {:path path})))))

(defn- entry-open-threads []
  (count (filter #(and (= "clio-entry-open" (.getName ^Thread %)) (.isAlive ^Thread %))
                 (.keySet (Thread/getAllStackTraces)))))

(defn- with-fifo-swapped-entry
  "Make the entry check see a regular file (`decoy`) while the entry itself is a
   FIFO with `mode`, i.e. a FIFO swapped in after the type check."
  [entry decoy mode f]
  (fs/delete-if-exists! entry)
  (mkfifo! entry mode)
  (let [file-key @#'fs/file-key]
    (with-redefs [fs/file-key (fn
                                ([p] (file-key p))
                                ([p links]
                                 ;; Parent descriptor paths have the same checked
                                 ;; basename. Do not fake the opened file's separate
                                 ;; /proc/self/fd/<n> identity check.
                                 (file-key (if (= (last (str/split p #"/"))
                                                  (last (str/split entry #"/")))
                                             decoy p)
                                           links)))]
      (f))))

(deftest jvm-fifo-swapped-in-after-the-entry-check-fails-closed
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            entry (ledger/segment-path segments (:event/id evt))
            decoy (str directory "/decoy.edn")]
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (fs/write-text! decoy (fs/read-text entry))
        (testing "a writable FIFO swap is refused without any blocked open"
          (with-fifo-swapped-entry
            entry decoy "0644"
            (fn []
              (let [before (entry-open-threads)
                    result (binding [fs/*entry-open-timeout-ms* 60000]
                             (future (error-code #(ledger/canonicalize-directory revisions segments []))))
                    outcome (deref result 15000 ::hung)]
                (when (= ::hung outcome)
                  (with-open [_ (java.io.FileOutputStream. ^String entry)]))
                (is (contains? #{:clio.fs/not-regular-file :clio.fs/entry-replaced} outcome))
                (is (= before (entry-open-threads)) "no opener thread is left blocked")))))
        (when-not (= "root" (System/getProperty "user.name"))
          (testing "a read-only FIFO swap uses the bounded fallback and caps blocked opens"
            (with-fifo-swapped-entry
              entry decoy "0444"
              (fn []
                (binding [fs/*entry-open-timeout-ms* 300]
                  (is (= :clio.fs/entry-open-blocked
                         (error-code #(ledger/canonicalize-directory revisions segments []))))
                  (let [stuck (entry-open-threads)
                        started (System/nanoTime)]
                    (is (= :clio.fs/entry-open-blocked
                           (error-code #(ledger/canonicalize-directory revisions segments []))))
                    (is (< (/ (- (System/nanoTime) started) 1e6) 250.0)
                        "the second attempt refuses at once instead of waiting")
                    (is (= stuck (entry-open-threads)) "no second blocked opener")))
                ;; Release the stuck reader; it closes its late channel.
                (java.nio.file.Files/setPosixFilePermissions
                 (java.nio.file.Paths/get entry (make-array String 0))
                 (java.nio.file.attribute.PosixFilePermissions/fromString "rw-r--r--"))
                (with-open [_ (java.io.FileOutputStream. ^String entry)])
                (loop [n 0]
                  (when (and (pos? (entry-open-threads)) (< n 100))
                    (Thread/sleep 20) (recur (inc n))))
                (is (zero? (entry-open-threads)))))))
        (fs/delete-if-exists! entry)))))

(deftest jvm-segment-directory-is-bound-and-never-empty
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [a (str directory "/store-a")
            b (str directory "/store-b")
            link (str directory "/segments-link")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            nio #(java.nio.file.Paths/get ^String % (make-array String 0))
            point! (fn [target]
                     (java.nio.file.Files/deleteIfExists (nio link))
                     (java.nio.file.Files/createSymbolicLink
                      (nio link) (nio target)
                      (make-array java.nio.file.attribute.FileAttribute 0)))]
        (testing "an empty segment-directory string is refused, not resolved to cwd"
          (let [cwd-before (set (fs/list-directory "."))]
            (is (= :clio.ledger/empty-directory-path
                   (error-code #(runtime/append-segment! runtime "" :record/observed fixture/facts))))
            (is (= :clio.ledger/empty-directory-path (error-code #(ledger/segment-paths ""))))
            (is (= :clio.ledger/empty-directory-path
                   (error-code #(ledger/create-segment! revisions "" evt))))
            (is (= cwd-before (set (fs/list-directory "."))))))
        (testing "enumeration and paths bind one real directory across a retarget"
          (fs/ensure-dir! a)
          (fs/ensure-dir! b)
          (is (= :appended (ledger/create-segment! revisions a evt)))
          (point! a)
          (let [list-directory fs/list-directory
                paths (with-redefs [fs/list-directory (fn [p]
                                                        (let [names (list-directory p)]
                                                          (point! b)
                                                          names))]
                        (ledger/segment-paths link))]
            (is (= [(str (fs/absolute-path a) "/" (last (str/split (first paths) #"/")))] paths))
            (is (every? #(str/starts-with? % (str (fs/absolute-path a) "/")) paths))))))))

(deftest jvm-segment-reads-reflush-the-directory-after-uncertain-repair
  ;; A stale-alias deletion whose directory force failed is invisible to a
  ;; retry, so every segment read fences the directory before returning.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            synced (atom [])
            sync! fs/sync-directory!]
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (let [expected (fs/directory-identity segments)]
          (with-redefs [fs/sync-directory!
                        (fn [p]
                          ;; Observe the actual directory while its descriptor is
                          ;; open, rather than comparing an ephemeral alias later.
                          (swap! synced conj (fs/directory-identity p))
                          (sync! p))]
            (is (= [evt] (:canonical/events (ledger/canonicalize-directory revisions segments [])))))
          (is (some #(= expected %) @synced)))))))

(deftest jvm-hard-link-added-while-publishing-is-refused
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            alias (str directory "/outside-alias")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            link! fs/hard-link!]
        (fs/ensure-dir! segments)
        ;; Another same-user process links the hidden temp during publication.
        (with-redefs [fs/hard-link! (fn [from to]
                                      (when (str/includes? from ".creating-") (link! from alias))
                                      (link! from to))]
          (is (= :clio.ledger/segment-hardlink
                 (error-code #(ledger/create-segment! revisions segments evt)))))))))

(deftest jvm-segment-publish-recovery-and-hardlink-guards
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            outside (str directory "/outside.edn")
            missing (str directory "/missing-segments")
            revisions (:schema/revisions (runtime/refresh runtime))
            {evt :event} (runtime/append! runtime (str directory "/events.edn")
                                          :record/observed fixture/facts)
            other (event/make-event (:schema/current runtime) :record/observed
                                    (assoc fixture/facts :event/stream "record:other"
                                           :event/subject "record:other"))
            facts (assoc fixture/facts :event/stream "record:m"
                         :event/subject "record:m")]
        (fs/ensure-dir! segments)
        (testing "new segments publish only after the event is written"
          (let [link! fs/hard-link!
                saw (atom false)]
            (with-redefs [fs/hard-link! (fn [from to]
                                         (is (fs/exists? from))
                                         (is (str/includes? from ".creating-"))
                                         (is (not (fs/exists? to)))
                                         (is (not (some #(= % to) (ledger/segment-paths segments))))
                                         (reset! saw true)
                                         (link! from to))]
              (is (= :appended (ledger/create-segment! revisions segments evt)))
              (is @saw))))
        (testing "setup failures preserve append recovery"
          (let [cause (try
                        (runtime/append-segment! runtime missing :record/observed facts)
                        nil
                        (catch Exception e e))
                recovery (:clio/append-recovery (ex-data cause))]
            (is (= :clio.ledger/missing-directory (:clio/error (ex-data cause))))
            (is (map? recovery))
            (is (some? (:event/id (:event recovery))))
            (is (= missing (:ledger/segment-directory recovery)))))
        (testing "hard-linked segment entries are refused"
          (ledger/create-ledger! outside)
          (let [linked (ledger/segment-path segments (:event/id other))]
            (fs/hard-link! outside linked)
            (is (= :clio.ledger/segment-hardlink
                   (error-code #(ledger/create-segment! revisions segments other))))
            (is (= "" (fs/read-text outside)))
            (is (= :clio.ledger/segment-hardlink
                   (error-code #(ledger/canonicalize-directory revisions segments []))))
            (fs/delete-if-exists! linked)))
        (is (= [evt] (:canonical/events
                      (runtime/canonicalize-directory runtime segments []))))))))

(defn- link-count [path]
  (long (java.nio.file.Files/getAttribute
         (java.nio.file.Paths/get ^String path (make-array String 0))
         "unix:nlink"
         (into-array java.nio.file.LinkOption [java.nio.file.LinkOption/NOFOLLOW_LINKS]))))

(defn- creating-entries [segments]
  (filterv #(str/includes? % ".creating-") (fs/list-directory segments)))

(deftest jvm-create-segment-binds-directory-across-symlink-retarget
  ;; Retarget the directory symlink after hard-link! publishes into store A but
  ;; before the temp delete / directory fence. Publication must stay on A's real
  ;; path (resolved once up front), not leave A multiply linked while fencing B.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [a (str directory "/store-a")
            b (str directory "/store-b")
            link (str directory "/segments-link")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            nio #(java.nio.file.Paths/get ^String % (make-array String 0))
            point! (fn [target]
                     (java.nio.file.Files/deleteIfExists (nio link))
                     (java.nio.file.Files/createSymbolicLink
                      (nio link) (nio target)
                      (make-array java.nio.file.attribute.FileAttribute 0)))
            link! fs/hard-link!]
        (fs/ensure-dir! a)
        (fs/ensure-dir! b)
        (point! a)
        (with-redefs [fs/hard-link! (fn [from to]
                                      (link! from to)
                                      (point! b))]
          (is (= :appended (ledger/create-segment! revisions link evt))))
        (let [seg (ledger/segment-path (fs/absolute-path a) (:event/id evt))]
          (is (= [evt] (ledger/read-ledger seg)))
          (is (= 1 (link-count seg)))
          (is (= [] (creating-entries a)))
          (is (= [] (fs/list-directory b)))
          (is (= [evt] (:canonical/events
                        (ledger/canonicalize-directory revisions a [])))))))))



(deftest jvm-reclaim-skips-non-uuid-creating-suffixes
  ;; Prefix-only matching would unlink operator backups such as
  ;; .<stem>.creating-backup after an exact retry.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            stem (#'ledger/encode-segment-stem (:event/id evt))
            backup (str segments "/." stem ".creating-backup")]
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (spit backup "{:note :operator-backup}\n")
        (is (= :already-present (ledger/create-segment! revisions segments evt)))
        (is (.exists (java.io.File. backup))
            "non-uuid creating suffix must survive reclaim")
        (is (empty?
             (filterv #(re-matches #".*\.creating-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}" %)
                      (fs/list-directory segments))))))))

(deftest jvm-create-segment-refuses-directory-replaced-mid-publish
  ;; Rename the bound store away and put a replacement at the same path after
  ;; the hard link; publication must fail closed rather than fence the impostor.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [store (str directory "/store")
            moved (str directory "/store-moved")
            impostor (str directory "/impostor")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            link! fs/hard-link!]
        (fs/ensure-dir! store)
        (fs/ensure-dir! impostor)
        (with-redefs [fs/hard-link! (fn [from to]
                                      (link! from to)
                                      (.renameTo (java.io.File. store) (java.io.File. moved))
                                      (.renameTo (java.io.File. impostor) (java.io.File. store)))]
          (is (= :clio.ledger/directory-replaced
                 (error-code #(ledger/create-segment! revisions store evt)))))
        (is (seq (creating-entries moved))
            "the original store keeps its in-flight publish state for recovery")))))

(deftest jvm-interrupted-publish-aliases-are-repaired-under-the-segment-lock
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            revisions (:schema/revisions (runtime/refresh runtime))
            make (fn [stream]
                   (event/make-event (:schema/current runtime) :record/observed
                                     (assoc fixture/facts :event/stream stream
                                            :event/subject stream)))
            evt (make "record:a")
            other (make "record:b")
            interrupt! (fn [e]
                         (let [path (ledger/segment-path segments (:event/id e))
                               name (last (str/split path #"/"))
                               stem (subs name 0 (- (count name) (count ".edn")))]
                           (fs/hard-link! path (str segments "/." stem ".creating-"
                                                    (host/random-uuid)))
                           path))]
        (fs/ensure-dir! segments)
        (testing "publication drops the temp alias before the directory fence"
          (let [path (ledger/segment-path segments (:event/id evt))
                sync! fs/sync-directory!
                overlaps (atom [])]
            (with-redefs [fs/sync-directory! (fn [dir]
                                               (when (and (fs/exists? path)
                                                          (seq (creating-entries segments)))
                                                 (swap! overlaps conj dir))
                                               (sync! dir))]
              (is (= :appended (ledger/create-segment! revisions segments evt))))
            (is (= [] @overlaps))
            (is (= 1 (link-count path)))))
        (testing "canonicalization repairs a stale temp alias of the same inode"
          (let [path (interrupt! evt)]
            (is (= 2 (link-count path)))
            (is (= [evt] (:canonical/events (ledger/canonicalize-directory revisions segments []))))
            (is (= [] (creating-entries segments)))
            (is (= 1 (link-count path)))))
        (testing "an exact retry repairs the alias and is :already-present"
          (let [path (interrupt! evt)]
            (is (= :already-present (ledger/create-segment! revisions segments evt)))
            (is (= [] (creating-entries segments)))
            (is (= 1 (link-count path)))))
        (testing "only this segment's own temp aliases of its own inode are removed"
          (is (= :appended (ledger/create-segment! revisions segments other)))
          (let [path (ledger/segment-path segments (:event/id other))
                other-stem (subs (last (str/split path #"/")) 0
                                 (- (count (last (str/split path #"/"))) (count ".edn")))
                evt-stem (subs (last (str/split (ledger/segment-path segments (:event/id evt)) #"/"))
                               0
                               (- (count (last (str/split (ledger/segment-path segments (:event/id evt)) #"/")))
                                  (count ".edn")))
                unrelated (str segments "/." other-stem ".creating-" (host/random-uuid))
                misnamed (str segments "/." evt-stem ".creating-" (host/random-uuid))]
            (ledger/create-ledger! unrelated)
            (fs/hard-link! path misnamed)
            (is (= :clio.ledger/segment-hardlink
                   (error-code #(ledger/canonicalize-directory revisions segments []))))
            (is (fs/exists? unrelated))
            (is (fs/exists? misnamed))
            (fs/delete-if-exists! misnamed)
            (fs/delete-if-exists! unrelated)
            (is (= 2 (count (:canonical/events
                             (ledger/canonicalize-directory revisions segments [])))))))))))

(deftest jvm-segment-recovery-paths-are-absolute-for-relative-directories
  (let [marker (str ".clio-jvm-seg-abs-" (host/random-uuid))
        directory (str (System/getProperty "user.dir") "/" marker)
        schemas (str directory "/schemas")
        segments-name (str "segments-" (host/random-uuid))
        segments-abs (str directory "/" segments-name)
        segments-rel (str marker "/" segments-name)]
    (try
      (fs/ensure-dir! segments-abs)
      (let [runtime (runtime/open schemas fixture/catalog)
            facts (assoc fixture/facts :event/stream "record:abs" :event/subject "record:abs")
            recovery (try
                       (with-redefs [fs/append-locked-text!
                                     (fn [_ _] (throw (ex-info "Injected append failure" {})))]
                         (runtime/append-segment! runtime segments-rel :record/observed facts))
                       nil
                       (catch Exception cause (:clio/append-recovery (ex-data cause))))]
        (is (string? (:ledger/segment-directory recovery)))
        (is (str/starts-with? (:ledger/segment-directory recovery) "/"))
        (is (not= segments-rel (:ledger/segment-directory recovery)))
        (is (str/ends-with? (:ledger/segment-directory recovery) segments-name))
        (is (str/starts-with? (:ledger/path recovery)
                              (:ledger/segment-directory recovery))))
      (finally (fs/remove-tree! directory)))))

(deftest jvm-ensure-segment-file-propagates-post-creation-durability-failures
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            event (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            path (ledger/segment-path segments (:event/id event))
            real-force! fs/force-file!
            real-sync! fs/sync-directory!
            file-forced? (atom false)]
        (fs/ensure-dir! segments)
        (with-redefs [fs/force-file! (fn [channel]
                                       (reset! file-forced? true)
                                       (real-force! channel))
                      fs/sync-directory! (fn [p]
                                           (if @file-forced?
                                             (throw (ex-info "Injected parent sync failure"
                                                             {:clio/error :clio.fs/directory-sync-unavailable
                                                              :path p}))
                                             (real-sync! p)))]
          (is (= :clio.fs/directory-sync-unavailable
                 (error-code #(ledger/ensure-segment-file! segments (:event/id event)))))
          (is (fs/exists? path)))))))

(deftest jvm-segment-filenames-preserve-case-variant-event-ids
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            single (str directory "/both.edn")
            lower "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
            upper "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE"
            revisions (:schema/revisions (runtime/refresh runtime))
            make (fn [id stream]
                   (assoc (event/make-event (:schema/current runtime) :record/observed
                                            (assoc fixture/facts :event/stream stream
                                                   :event/subject stream))
                          :event/id id))
            e-lower (make lower "record:lo")
            e-upper (make upper "record:up")
            lower-name (last (str/split (ledger/segment-path segments lower) #"/"))
            upper-name (last (str/split (ledger/segment-path segments upper) #"/"))]
        (fs/ensure-dir! segments)
        (ledger/create-ledger! single)
        (is (not= (str/lower-case lower-name) (str/lower-case upper-name)))
        (is (= :appended (ledger/append-event! revisions single e-lower)))
        (is (= :appended (ledger/append-event! revisions single e-upper)))
        (is (= :appended (ledger/create-segment! revisions segments e-lower)))
        (is (= :appended (ledger/create-segment! revisions segments e-upper)))
        (is (= (:canonical/events (ledger/canonicalize-files revisions [single]))
               (:canonical/events (ledger/canonicalize-directory revisions segments []))))))))

(deftest jvm-unpublished-creating-temps-are-reclaimed-before-retry
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            revisions (:schema/revisions (runtime/refresh runtime))
            event (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            path (ledger/segment-path segments (:event/id event))
            name (last (str/split path #"/"))
            stem (subs name 0 (- (count name) (count ".edn")))
            orphan (str segments "/." stem ".creating-" (host/random-uuid))]
        (fs/ensure-dir! segments)
        (ledger/create-ledger! orphan)
        (is (seq (creating-entries segments)))
        (is (not (fs/exists? path)))
        (is (= :appended (ledger/create-segment! revisions segments event)))
        (is (= [] (creating-entries segments)))))))

(deftest jvm-reclaim-skips-uppercase-or-non-v4-uuid-creating-suffixes
  ;; publish-temp-path only emits lowercase version-4 UUIDs (random-uuid), so
  ;; any other UUID spelling is not a Clio temp and must survive reclaim.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            stem (#'ledger/encode-segment-stem (:event/id evt))
            upper (str segments "/." stem ".creating-AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE")
            v1 (str segments "/." stem ".creating-aaaaaaaa-bbbb-1ccc-8ddd-eeeeeeeeeeee")]
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (spit upper "{:note :operator-upper}\n")
        (spit v1 "{:note :operator-v1}\n")
        (is (= :already-present (ledger/create-segment! revisions segments evt)))
        (is (.exists (java.io.File. upper)) "uppercase UUID suffix must survive reclaim")
        (is (.exists (java.io.File. v1)) "non-v4 UUID suffix must survive reclaim")))))

(deftest jvm-try-entry-lock-on-owned-path-releases-its-guard-hold
  ;; A nil try-acquire on a path this thread already locks must not leave an
  ;; extra process-guard hold behind once the original token is released.
  (with-ledger
    (fn [{:keys [path]}]
      (let [guard @#'fs/process-guard
            token (fs/acquire-entry-lock! path)]
        (is (nil? (fs/try-acquire-entry-lock! path)))
        (fs/release-lock! token)
        (is (not (.isLocked ^java.util.concurrent.locks.ReentrantLock guard))
            "process guard must be fully released")))))

(deftest jvm-fd-position-tag-stays-below-small-filesystem-limits
  ;; vfat's s_maxbytes is 0xffffffff; ext4 with 1 KiB blocks is about 2^34.
  (let [lo (some-> (resolve 'clio.extern.jvm.fs/fd-tag-min) deref)
        hi (some-> (resolve 'clio.extern.jvm.fs/fd-tag-max) deref)]
    (is (and lo hi (< 0 lo hi 0xffffffff)) "position tag range must stay below 2^32 - 1")))

(deftest jvm-append-segment-refuses-unresolvable-existing-directory
  ;; Only a confirmed missing path may use the lexical (symlink-spelling)
  ;; recovery; EACCES/ELOOP/I/O failures must fail before an event exists.
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            _ (fs/ensure-dir! segments)
            outcome (try
                      (with-redefs [fs/absolute-path
                                    (fn [p] (throw (java.nio.file.AccessDeniedException. p)))]
                        (runtime/append-segment! runtime segments :record/observed fixture/facts))
                      :no-error
                      (catch Exception cause cause))]
        (is (instance? Exception outcome))
        (is (not (contains? (ex-data outcome) :clio/append-recovery))
            "no recovery token (no event) for a non-missing resolution failure")))))

(deftest jvm-interrupted-bounded-open-is-abandoned-and-counted
  ;; An interrupt while waiting must take the same abandonment path as a
  ;; timeout, so the stuck-open cap still applies and a late channel is closed.
  (let [directory (str "/tmp/clio-jvm-fifo-" (host/random-uuid))
        fifo (str directory "/entry.edn")
        stuck @#'fs/stuck-entry-opens
        before @stuck]
    (try
      (fs/ensure-dir! directory)
      (is (zero? (.waitFor (.start (ProcessBuilder. ^java.util.List ["mkfifo" fifo])))))
      (let [options (into-array java.nio.file.OpenOption [java.nio.file.StandardOpenOption/READ])
            outcome (try
                      (.interrupt (Thread/currentThread))
                      (#'fs/open-entry-bounded! fifo options fifo)
                      :opened
                      (catch Throwable cause cause)
                      (finally (Thread/interrupted)))]
        (is (instance? Throwable outcome))
        (is (= (inc before) @stuck) "interrupted wait must count as a stuck open"))
      (finally
        ;; Release the blocked reader so the late channel closes and the count drops.
        (try (with-open [_ (java.io.FileOutputStream. fifo)]) (catch Exception _ nil))
        (loop [n 0] (when (and (< n 100) (not= before @stuck)) (Thread/sleep 20) (recur (inc n))))
        (fs/remove-tree! directory)))))

(deftest jvm-read-only-segment-replay-skips-only-unsupported-preflight
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            revisions (:schema/revisions runtime)
            evt (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            forces (atom 0)]
        (fs/ensure-dir! segments)
        (ledger/create-segment! revisions segments evt)
        (with-redefs [fs/require-directory-sync-support!
                      (fn [_] (throw (ex-info "Unsupported platform"
                                             {:clio/error :clio.fs/directory-sync-unavailable})))
                      fs/sync-directory!
                      (fn [_] (swap! forces inc)
                        (throw (ex-info "Unexpected read-only force"
                                        {:clio/error :unexpected-directory-force})))]
          (is (= {:events [evt]}
                 (try {:events (:canonical/events (ledger/canonicalize-directory revisions segments []))}
                      (catch Exception cause {:error (:clio/error (ex-data cause))}))))
          (is (zero? @forces)))
        (with-redefs [fs/sync-directory!
                      (fn [_] (throw (ex-info "Actual force failure without a wrapped cause"
                                             {:clio/error :clio.fs/directory-sync-unavailable})))]
          (is (= :clio.fs/directory-sync-unavailable
                 (error-code #(ledger/canonicalize-directory revisions segments [])))))))))

(deftest jvm-segment-lock-preserves-a-lost-claim-after-its-temp-is-unlinked
  (with-ledger
    (fn [{:keys [directory]}]
      (let [temp (str directory "/creating-temp.edn")]
        (ledger/create-ledger! temp)
        (is (= {:result :clio.infra.ledger/claim-lost}
               (try
                 {:result (#'ledger/with-segment-lock
                           fs/acquire-entry-lock! temp false (constantly nil)
                           (fn [lock]
                             (is (true? (fs/delete-locked-entry-if-owns! lock temp)))
                             (is (zero? (fs/locked-link-count lock)))
                             :clio.infra.ledger/claim-lost))}
                 (catch Exception cause {:error (:clio/error (ex-data cause))}))))
        (is (not (fs/exists? temp)))
        (ledger/create-ledger! temp)
        (let [lock (fs/acquire-entry-lock! temp)]
          (is (some? lock) "the lost-claim path released its descriptor")
          (fs/release-lock! lock))))))

(deftest jvm-segment-lock-rechecks-link-count-before-release
  ;; A hard link added while the segment lock is held must not let the
  ;; operation succeed and acknowledge a now multiply linked segment.
  (with-ledger
    (fn [{:keys [directory]}]
      (let [segment (str directory "/seg.edn")
            alias (str directory "/alias.edn")]
        (ledger/create-ledger! segment)
        (is (= :clio.ledger/segment-hardlink
               (error-code
                #(#'ledger/with-segment-lock
                  fs/acquire-entry-lock! segment false
                  (fn [_]
                    (java.nio.file.Files/createLink (java.nio.file.Paths/get alias (make-array String 0)) (java.nio.file.Paths/get segment (make-array String 0)))
                    :ok)))))))))

(deftest jvm-ensure-segment-file-binds-directory-across-symlink-retarget
  ;; A retarget between the existence check and creation must not create the
  ;; segment in another store.
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [a (str directory "/a") b (str directory "/b") link (str directory "/link")
            p (fn [s] (java.nio.file.Paths/get s (make-array String 0)))
            event (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            name (last (str/split (ledger/segment-path a (:event/id event)) #"/"))
            create! ledger/create-ledger!]
        (fs/ensure-dir! a)
        (fs/ensure-dir! b)
        (java.nio.file.Files/createSymbolicLink (p link) (p a) (make-array java.nio.file.attribute.FileAttribute 0))
        (with-redefs [ledger/create-ledger!
                      (fn [path]
                        (java.nio.file.Files/delete (p link))
                        (java.nio.file.Files/createSymbolicLink (p link) (p b) (make-array java.nio.file.attribute.FileAttribute 0))
                        (create! path))]
          (try (ledger/ensure-segment-file! link (:event/id event)) (catch Exception _ nil)))
        (is (not (.exists (java.io.File. (str b "/" name))))
            "segment must not be created in the retargeted store")))))

(defn- nio [s] (java.nio.file.Paths/get s (make-array String 0)))

(defn- segment-stem [segments id]
  (let [name (last (str/split (ledger/segment-path segments id) #"/"))]
    (subs name 0 (- (count name) (count ".edn")))))

(deftest jvm-segment-lock-refuses-a-relocated-locked-inode
  ;; Rename the locked inode to another one-link name and install a replacement
  ;; at the segment path while f runs: link count stays 1, but the path no
  ;; longer names the locked inode, so the operation must not be acknowledged.
  (with-ledger
    (fn [{:keys [directory]}]
      (let [segment (str directory "/seg.edn")
            moved (str directory "/moved.edn")]
        (ledger/create-ledger! segment)
        (is (= :clio.ledger/segment-relocated
               (error-code
                #(#'ledger/with-segment-lock
                  fs/acquire-entry-lock! segment false
                  (fn [_]
                    (java.nio.file.Files/move (nio segment) (nio moved) (make-array java.nio.file.CopyOption 0))
                    (spit segment "")
                    :ok)))))))))

(deftest jvm-reclaim-never-unlinks-a-replacement-at-the-temp-path
  ;; The entry lock pins an inode: if the locked temp is renamed away and an
  ;; unrelated file installed at its name, reclaim must leave the replacement.
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [segments (str directory "/segments")
            evt (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            id (:event/id evt)
            final (ledger/segment-path segments id)
            tmp (str segments "/." (segment-stem segments id) ".creating-" (host/random-uuid))
            moved (str directory "/moved-temp")
            try-lock! fs/try-acquire-entry-lock!]
        (fs/ensure-dir! segments)
        (ledger/create-ledger! final)
        (ledger/create-ledger! tmp)
        (with-redefs [fs/try-acquire-entry-lock!
                      (fn [p]
                        (let [lock (try-lock! p)]
                          (when (and lock (= p tmp))
                            (java.nio.file.Files/move (nio tmp) (nio moved) (make-array java.nio.file.CopyOption 0))
                            (spit tmp "{:unrelated true}\n"))
                          lock))]
          (#'ledger/reclaim-unpublished-temps! segments id))
        (is (= "{:unrelated true}\n" (slurp tmp))
            "replacement at the temp pathname must survive reclaim")))))

(deftest jvm-publish-sweep-keeps-a-distinct-concurrent-temp
  ;; After an external hard link lands on the published inode, the post-publish
  ;; sweep may only drop same-inode creating aliases, never another publisher's
  ;; distinct-inode temp for the same event id.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            id (:event/id evt)
            other-temp (str segments "/." (segment-stem segments id) ".creating-" (host/random-uuid))
            external (str directory "/external-link.edn")
            link! fs/hard-link!]
        (fs/ensure-dir! segments)
        (with-redefs [fs/hard-link! (fn [from to]
                                      (link! from to)
                                      (link! from external)
                                      (spit other-temp "{:other :publisher}\n"))]
          (is (= :clio.ledger/segment-hardlink
                 (error-code #(ledger/create-segment! revisions segments evt)))))
        (is (fs/exists? other-temp)
            "a distinct-inode concurrent temp must not be swept")))))

(deftest jvm-ensure-segment-file-rechecks-directory-on-existing-file-path
  ;; If the bound directory is replaced by one that already holds the segment,
  ;; the existing-file fast path must still fail closed.
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [store (str directory "/store")
            moved (str directory "/store-moved")
            impostor (str directory "/impostor")
            evt (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            id (:event/id evt)
            swapped (atom false)
            symlink? fs/symlink?]
        (fs/ensure-dir! store)
        (fs/ensure-dir! impostor)
        (ledger/create-ledger! (ledger/segment-path impostor id))
        (with-redefs [fs/symlink? (fn [p]
                                    (when (compare-and-set! swapped false true)
                                      (.renameTo (java.io.File. store) (java.io.File. moved))
                                      (.renameTo (java.io.File. impostor) (java.io.File. store)))
                                    (symlink? p))]
          (is (= :clio.ledger/directory-replaced
                 (error-code #(ledger/ensure-segment-file! store id)))))))))

(deftest jvm-read-directory-retains-the-directory-binding
  ;; A rename+replace after enumeration must not feed same-named valid
  ;; segments from the replacement store into canonicalization.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [store (str directory "/store")
            moved (str directory "/store-moved")
            impostor (str directory "/impostor")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            swapped (atom false)
            symlink? fs/symlink?]
        (fs/ensure-dir! store)
        (fs/ensure-dir! impostor)
        (is (= :appended (ledger/create-segment! revisions store evt)))
        (let [name (last (str/split (ledger/segment-path store (:event/id evt)) #"/"))]
          (java.nio.file.Files/copy (nio (str store "/" name)) (nio (str impostor "/" name))
                                    (make-array java.nio.file.CopyOption 0)))
        (with-redefs [fs/symlink? (fn [p]
                                    (when (compare-and-set! swapped false true)
                                      (.renameTo (java.io.File. store) (java.io.File. moved))
                                      (.renameTo (java.io.File. impostor) (java.io.File. store)))
                                    (symlink? p))]
          (is (= :clio.ledger/directory-replaced
                 (error-code #(ledger/read-directory store [])))))))))

(deftest jvm-lasting-segment-hardlink-fails-closed-promptly
  ;; A lasting external link must not block create-segment! for two nested
  ;; 10-second admission retries.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))]
        (fs/ensure-dir! segments)
        (is (= :appended (ledger/create-segment! revisions segments evt)))
        (fs/hard-link! (ledger/segment-path segments (:event/id evt)) (str directory "/outside.edn"))
        (let [started (System/currentTimeMillis)
              code (error-code #(ledger/create-segment! revisions segments evt))
              elapsed (- (System/currentTimeMillis) started)]
          (is (= :clio.ledger/segment-hardlink code))
          (is (< elapsed 2000) (str "lasting hard link took " elapsed " ms")))))))

(deftest jvm-entry-lock-reports-an-unlinked-inode-distinctly
  ;; nlink 0 after open means a concurrent reclaim unlinked the entry, not an
  ;; extra hard link: report :clio.fs/unlinked-entry.
  (with-ledger
    (fn [{:keys [directory]}]
      (let [entry (str directory "/entry.edn")]
        (ledger/create-ledger! entry)
        (with-redefs [fs/fd-link-count (fn [_ _] 0)]
          (is (= :clio.fs/unlinked-entry
                 (error-code #(fs/acquire-entry-lock! entry)))))))))

(deftest jvm-temp-unlinked-by-a-winner-is-a-lost-claim
  ;; The winner published and reclaimed the loser's temp between the loser's
  ;; open and lock check: the loser must admit through the final segment.
  (with-ledger
    (fn [{:keys [directory path runtime]}]
      (let [segments (str directory "/segments")
            {evt :event} (runtime/append! runtime path :record/observed fixture/facts)
            revisions (:schema/revisions (runtime/refresh runtime))
            acquire! fs/acquire-entry-lock!
            raced (atom false)]
        (fs/ensure-dir! segments)
        (with-redefs [fs/acquire-entry-lock!
                      (fn [p & more]
                        (if (and (str/includes? p ".creating-")
                                 (compare-and-set! raced false true))
                          (do (is (= :appended (ledger/create-segment! revisions segments evt)))
                              (throw (ex-info "Directory entry was unlinked"
                                              {:path p :nlink 0
                                               :clio/error :clio.fs/unlinked-entry})))
                          (apply acquire! p more)))]
          (is (= :already-present (ledger/create-segment! revisions segments evt))))))))

(deftest jvm-try-entry-lock-binds-the-opened-descriptor
  ;; An entry swapped between the path stat and the open must not yield a token
  ;; whose recorded identity differs from the locked descriptor's inode.
  (with-ledger
    (fn [{:keys [directory]}]
      (let [entry (str directory "/entry.edn")
            file-key @#'fs/file-key]
        (ledger/create-ledger! entry)
        (with-redefs [fs/file-key (fn
                                    ([p] (file-key p))
                                    ([p links] (if (str/starts-with? (str p) "/proc/")
                                                 (file-key p links)
                                                 [:swapped-inode p])))]
          (let [token (fs/try-acquire-entry-lock! entry)]
            (when token (fs/release-lock! token))
            (is (nil? token) "a descriptor/path identity mismatch must not lock")))))))

(deftest jvm-ensure-segment-file-reflushes-an-existing-entry
  ;; A retry after a failed force/sync must not acknowledge without reflushing.
  (with-ledger
    (fn [{:keys [directory runtime]}]
      (let [store (str directory "/store")
            evt (event/make-event (:schema/current runtime) :record/observed fixture/facts)
            synced (atom 0)
            sync-locked! fs/sync-locked!]
        (fs/ensure-dir! store)
        (ledger/create-ledger! (ledger/segment-path store (:event/id evt)))
        (with-redefs [fs/sync-locked! (fn [lock] (swap! synced inc) (sync-locked! lock))]
          (ledger/ensure-segment-file! store (:event/id evt)))
        (is (pos? @synced) "existing segment must be reflushed under its lock")))))

(deftest jvm-read-only-open-detection-does-not-depend-on-provider-text
  ;; EROFS may arrive with a provider-defined or localized reason; the bounded
  ;; READ fallback must key off the file store, not English text.
  (with-ledger
    (fn [{:keys [directory]}]
      (let [entry (str directory "/entry.edn")
            localized (java.nio.file.FileSystemException. entry nil "Système de fichiers accessible en lecture seulement")]
        (ledger/create-ledger! entry)
        (with-redefs [fs/file-store-read-only? (fn [_] true)]
          (is (true? (boolean (#'fs/read-only-target-error? localized entry)))))
        (with-redefs [fs/file-store-read-only? (fn [_] false)]
          (is (false? (boolean (#'fs/read-only-target-error? localized entry)))
              "a writable store with an unrelated error still rethrows"))))))

(deftest jvm-identity-checked-unlink-survives-a-swap-after-the-check
  ;; A replacement installed after the identity check but before the unlink
  ;; must survive: the unlink acts on a private quarantine name whose inode was
  ;; verified, never on the shared pathname.
  (with-ledger
    (fn [{:keys [directory]}]
      (let [entry (str directory "/.temp.creating-x")
            moved (str directory "/moved-away")
            owns? fs/locked-entry-owns-path?]
        (ledger/create-ledger! entry)
        (let [lock (fs/acquire-entry-lock! entry)]
          (try
            (with-redefs [fs/locked-entry-owns-path?
                          (fn [l p]
                            (let [r (owns? l p)]
                              (when (and r (= p entry) (not (fs/exists? moved)))
                                (java.nio.file.Files/move (nio entry) (nio moved) (make-array java.nio.file.CopyOption 0))
                                (spit entry "{:replacement true}\n"))
                              r))]
              (is (false? (fs/delete-locked-entry-if-owns! lock entry))))
            (finally (fs/release-lock! lock))))
        (is (= "{:replacement true}\n" (slurp entry)) "replacement must survive")
        (is (= [] (filterv #(str/includes? % ".reclaim-") (fs/list-directory directory)))
            "no quarantine name is left behind")))))
