(ns clio.infra.ledger
  (:require [clio.domain.canonicalize :as canonicalize]
            [clio.domain.schema :as schema]
            #?(:clj [clio.extern.jvm.fs :as fs]
               :cljs [clio.extern.js.fs :as fs])
            [clio.law.ledger :as ledger-law]
            [clio.shape.edn :as edn]
            [clojure.string :as str]))

(defn- fail!
  [type message data]
  (throw (ex-info message (assoc data :clio/error type))))

(defn create-ledger!
  [path]
  (fs/create-exclusive! path))

(defn- parse-ledger-text
  [path text]
  (->> text
       str/split-lines
       (map-indexed vector)
       (remove (fn [[_ line]] (str/blank? line)))
       (mapv
        (fn [[index line]]
          (try
            (edn/read-one line)
            (catch #?(:clj Exception :cljs :default) cause
              (fail! :clio.ledger/invalid-edn
                     "Ledger line must contain exactly one readable EDN form"
                     {:path path
                      :line (inc index)
                      :cause (str cause)})))))))

(defn- read-existing-ledger!
  "Capture and parse one complete immutable snapshot through its owning descriptor."
  [path]
  (let [lock (fs/acquire-read-lock! path)]
    (try
      (parse-ledger-text path (fs/read-locked-text lock))
      (finally (fs/release-lock! lock)))))

(defn read-ledger
  "Inspect one ledger under its writer lock; an absent optional ledger remains empty."
  [path]
  (if-not (fs/exists? path)
    []
    (read-existing-ledger! path)))

(defn- append-record!
  [lock existing-text event]
  (let [delimiter (if (or (str/blank? existing-text)
                          (str/ends-with? existing-text "\n"))
                    ""
                    "\n")]
    (fs/append-locked-text! lock (str delimiter (pr-str event) "\n"))))

(defn- require-ledger-path!
  "A caller naming an explicit partition file means it to exist. Treating a
   misspelled, deleted, or unmounted path as an empty ledger would let
   canonicalization silently produce a plausible but incomplete history, and
   would let an append open a fresh ledger while the intended one stays
   behind. `create-ledger!` is the only path that brings a ledger into being."
  [path]
  (when-not (fs/exists? path)
    (fail! :clio.ledger/missing-file
           "Ledger file does not exist"
           {:path path})))

(defn append-event!
  "Append one validated event while holding an OS-backed exclusive lock on
   the ledger inode. Exact retries are idempotent. Causal parents may live in
   other physical ledger files; complete-history causality is checked when
   ledgers are unioned."
  [revisions path event]
  (require-ledger-path! path)
  (let [lock (fs/acquire-lock! path)]
    (try
      (schema/validate-event! revisions event)
      (let [existing-text (fs/read-locked-text lock)
            events (parse-ledger-text path existing-text)]
        (doseq [existing events]
          (schema/validate-event! revisions existing))
        ;; The admission decision is a law, not transport; this namespace only
        ;; carries its verdict back across the boundary as an error or a write.
        (let [{:admission/keys [verdict conflict]}
              (ledger-law/append-admission events event)]
          (case verdict
            :already-present
            (do (fs/sync-locked! lock) :already-present)

            :id-collision
            (fail! :clio.ledger/id-collision
                   "Ledger already contains different data for this event id"
                   {:old conflict :new event})

            :stream-slot-conflict
            (fail! :clio.ledger/concurrent-stream-write
                   "Ledger already contains a different event at this stream revision"
                   {:old conflict :new event})

            :appendable
            (do
              (append-record! lock existing-text event)
              :appended))))
      (finally
        (fs/release-lock! lock)))))

(defn ensure-durable-with!
  "Read under the owning lock, then load revisions, validate and reflush before release.
   Loading after capture admits concurrently published schemas without weakening
   validation. The loader must be synchronous and must not acquire this ledger lock."
  [load-revisions path]
  (require-ledger-path! path)
  (let [lock (fs/acquire-lock! path)]
    (try
      (let [events (parse-ledger-text path (fs/read-locked-text lock))
            revisions (load-revisions)]
        (doseq [existing events]
          (schema/validate-event! revisions existing)))
      (fs/sync-locked! lock)
      (finally (fs/release-lock! lock)))))

(defn ensure-durable!
  "Validate and reflush an existing ledger against explicitly supplied revisions.
   Visible history can contain a creation or append whose synchronization failed. The
   owning lock remains held through validation and the new durability fence."
  [revisions path]
  (ensure-durable-with! (fn [] revisions) path))

(defn read-ledgers
  "Capture complete per-file snapshots under the same inode locks used by writers.
   Pure validation/union consumes these immutable values after releasing each lock;
   this does not imply one atomic transaction across separate partition files."
  [paths]
  (doseq [path paths] (require-ledger-path! path))
  (mapv read-existing-ledger! paths))

(defn canonicalize-files
  [revisions paths]
  (canonicalize/canonicalize revisions (read-ledgers paths)))

;; Segment directories: one immutable ledger file per event. Physical
;; partitioning carries no semantic authority (law 6), so a directory of
;; segments canonicalizes exactly like the same events in one file.

(def ^:private segment-suffix ".edn")

(defn- require-directory!
  [directory]
  (when-not (fs/exists? directory)
    (fail! :clio.ledger/missing-directory
           "Segment directory does not exist"
           {:path directory})))

(defn segment-path
  "The content-addressed segment file for one event id inside `directory`."
  [directory event-id]
  (when-not (and (string? event-id)
                 (re-matches #"[0-9A-Za-z][0-9A-Za-z-]*" event-id))
    (fail! :clio.ledger/invalid-segment-id
           "Segment event id must be a plain identifier"
           {:event/id event-id}))
  (str directory "/" event-id segment-suffix))

(defn segment-paths
  "Every visible `*.edn` segment in `directory`, sorted only for deterministic
   reading; order has no meaning. A missing directory is an error, never an
   empty history."
  [directory]
  (require-directory! directory)
  (->> (fs/list-files directory)
       (filter #(and (str/ends-with? % segment-suffix)
                     (not (str/starts-with? % "."))))
       sort
       (mapv #(str directory "/" %))))

(defn ensure-segment-file!
  "Bring the segment file for `event-id` into being exactly once. A concurrent
   or retried creator that finds it already present is not an error. The segment
   directory must already exist — append never silently creates a replacement
   for a typo, deletion, or unmounted volume."
  [directory event-id]
  (require-directory! directory)
  (let [path (segment-path directory event-id)]
    (when (fs/exists? path)
      (when (fs/symlink? path)
        (fail! :clio.ledger/segment-symlink
               "Segment entry must not be a symbolic link"
               {:path path})))
    (when-not (fs/exists? path)
      (try
        (create-ledger! path)
        (catch #?(:clj Exception :cljs :default) cause
          (when-not (fs/exists? path) (throw cause)))))
    path))

(defn create-segment!
  "Write one validated event as its own segment. The segment is named by the
   event id, so an exact retry is :already-present and a different event with
   the same id is an :id-collision; other segments are never touched. Unlike
   ordinary ledger append, a preexisting segment never admits a second event."
  [revisions directory event]
  (schema/validate-event! revisions event)
  (let [path (ensure-segment-file! directory (:event/id event))
        existing (read-existing-ledger! path)]
    (cond
      (empty? existing)
      (append-event! revisions path event)
      (= 1 (count existing))
      (if (= (first existing) event)
        :already-present
        (fail! :clio.ledger/id-collision
               "Segment already holds a different event"
               {:path path :event/id (:event/id event)}))
      :else
      (fail! :clio.ledger/multi-event-segment
             "Segment file must hold exactly one event"
             {:path path :count (count existing)}))))

(defn- require-segment-file!
  "Each segment is one non-empty, non-symlink `*.edn` file named by its event id."
  [_directory path]
  (when (fs/symlink? path)
    (fail! :clio.ledger/segment-symlink
           "Segment entry must not be a symbolic link"
           {:path path}))
  (let [name (last (str/split path #"/"))
        expected-id (when (and name (str/ends-with? name segment-suffix))
                      (subs name 0 (- (count name) (count segment-suffix))))
        events (read-existing-ledger! path)]
    (when (empty? events)
      (fail! :clio.ledger/empty-segment
             "Segment file is empty (interrupted create?)"
             {:path path}))
    (when-not (= 1 (count events))
      (fail! :clio.ledger/multi-event-segment
             "Segment file must hold exactly one event"
             {:path path :count (count events)}))
    (when-not (= expected-id (:event/id (first events)))
      (fail! :clio.ledger/segment-id-mismatch
             "Segment filename must match its event id"
             {:path path :event/id (:event/id (first events))}))))

(defn canonicalize-directory
  "Canonicalize the extra ledger files (for example a frozen legacy ledger)
   together with every segment in `directory`. Each segment is validated for
   the one-event-per-`<event-id>.edn` format before the ordinary reader runs."
  [revisions directory extra-paths]
  (let [paths (segment-paths directory)]
    (doseq [path paths] (require-segment-file! directory path))
    (canonicalize-files revisions (into (vec extra-paths) paths))))
