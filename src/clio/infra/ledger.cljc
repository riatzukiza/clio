(ns clio.infra.ledger
  (:require [clio.domain.canonicalize :as canonicalize]
            [clio.domain.schema :as schema]
            #?(:clj [clio.extern.jvm.fs :as fs]
               :cljs [clio.extern.js.fs :as fs])
            [clio.law.event :as event-law]
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
  ;; An empty spelling would resolve to the process working directory.
  (when (or (not (string? directory)) (str/blank? directory))
    (fail! :clio.ledger/empty-directory-path
           "Segment directory path must not be empty"
           {:path directory}))
  (when-not (fs/exists? directory)
    (fail! :clio.ledger/missing-directory
           "Segment directory does not exist"
           {:path directory})))

(defn- bind-segment-directory!
  "Resolve `directory` once to its real path after the existence check and capture
   its device/inode. Publish, reclaim, and retry paths all use this binding so a
   symlink retarget or directory rename+replace cannot silently redirect the
   operation onto another store."
  [directory]
  (require-directory! directory)
  (let [real (try
               (fs/absolute-path directory)
               (catch #?(:clj Exception :cljs :default) cause
                 (if (fs/missing-path-error? cause)
                   (fail! :clio.ledger/missing-directory
                          "Segment directory does not exist"
                          {:path directory})
                   (throw cause))))]
    [real (fs/directory-identity real)]))

(defn- ensure-bound-directory!
  "Fail closed when the bound directory inode no longer sits at `directory`."
  [directory identity]
  (when-not (fs/same-directory-identity? directory identity)
    (fail! :clio.ledger/directory-replaced
           "Segment directory was replaced during the operation"
           {:path directory}))
  directory)

(defn- upper-char?
  [ch]
  (let [s (str ch)]
    (not= s (str/lower-case s))))

(defn- encode-segment-stem
  "Map an event id to a directory-entry stem that stays distinct under
   case-folding filesystems. Lowercase UUID text is unchanged; each uppercase
   letter becomes `!<lower>` so case-variant ids never alias as one filename."
  [event-id]
  (->> event-id
       (map (fn [ch]
              (let [s (str ch)]
                (if (upper-char? ch)
                  (str "!" (str/lower-case s))
                  s))))
       (apply str)))

(defn- decode-segment-stem
  "Inverse of encode-segment-stem. Rejects a truncated `!` escape."
  [stem]
  (let [n (count stem)]
    (loop [i 0 out ""]
      (if (>= i n)
        out
        (let [ch (subs stem i (inc i))]
          (if (= "!" ch)
            (if (>= (inc i) n)
              (fail! :clio.ledger/invalid-segment-id
                     "Segment filename has a truncated case escape"
                     {:stem stem})
              (recur (+ i 2)
                     (str out (str/upper-case (subs stem (inc i) (+ i 2))))))
            (recur (inc i) (str out ch))))))))

(defn segment-path
  "The content-addressed segment file for one event id inside `directory`.
   The filename rule admits exactly the ids an ordinary ledger admits
   (`clio.law.event/uuid-string?`), encoded so case-variant ids remain distinct
   even on case-folding filesystems. Lowercase UUID text keeps the plain
   `<event-id>.edn` spelling."
  [directory event-id]
  (when-not (event-law/uuid-string? event-id)
    (fail! :clio.ledger/invalid-segment-id
           "Segment event id must be a plain identifier"
           {:event/id event-id}))
  (str directory "/" (encode-segment-stem event-id) segment-suffix))

(defn- edn-suffix-case-variant?
  "True when `name` ends with a case-folded `.edn` that is not the canonical
   lowercase suffix. On a case-insensitive filesystem such an entry aliases the
   canonical segment path, so enumeration must refuse it rather than omit it."
  [name]
  (and (string? name)
       (>= (count name) (count segment-suffix))
       (= (str/lower-case (subs name (- (count name) (count segment-suffix))))
          segment-suffix)
       (not (str/ends-with? name segment-suffix))))

(defn segment-paths
  "Every visible `*.edn` segment in `directory`, sorted only for deterministic
   reading; order has no meaning. A missing directory — including one that
   disappears between the existence check and enumeration — is an error, never
   an empty history. Case-variant suffixes (`.EDN`, `.Edn`, …) are rejected so
   they cannot silently disappear from canonicalization on case-folding hosts."
  [directory]
  ;; Resolve once: enumeration and every returned path name the same real
  ;; directory, so a symlink retarget after listing cannot mix two stores.
  (let [[directory identity] (bind-segment-directory! directory)
        _ (ensure-bound-directory! directory identity)
        names (try
                (fs/list-directory directory)
                (catch #?(:clj Exception :cljs :default) cause
                  (if (fs/missing-path-error? cause)
                    (fail! :clio.ledger/missing-directory
                           "Segment directory does not exist"
                           {:path directory})
                    (throw cause))))
        _ (ensure-bound-directory! directory identity)
        variants (filterv edn-suffix-case-variant? names)]
    (when (seq variants)
      (fail! :clio.ledger/noncanonical-segment-suffix
             "Segment filename must use lowercase .edn suffix"
             {:path directory :names variants}))
    (->> names
         (filter #(and (str/ends-with? % segment-suffix)
                       (not (str/starts-with? % "."))))
         sort
         (mapv #(str directory "/" %)))))

(defn- refuse-segment-symlink!
  [path]
  (when (fs/symlink? path)
    (fail! :clio.ledger/segment-symlink
           "Segment entry must not be a symbolic link"
           {:path path})))

(def ^:private creating-marker ".creating-")

(defn- segment-stem-from-filename
  [name]
  (when (and name (str/ends-with? name segment-suffix))
    (subs name 0 (- (count name) (count segment-suffix)))))

(defn- publish-temp-path
  [directory event-id]
  (str directory "/." (encode-segment-stem event-id) creating-marker (random-uuid)))

(defn- publish-temp-basename?
  "True when `name` is exactly `.<stem>.creating-<uuid>` — the grammar
   publish-temp-path emits. Prefix-only matching would reclaim operator backups
   such as `.<stem>.creating-backup`."
  [stem name]
  (let [prefix (str "." stem creating-marker)]
    (and (string? name)
         (str/starts-with? name prefix)
         (event-law/uuid-string? (subs name (count prefix))))))

(defn- stale-publish-aliases
  "Name the hidden publish temps of the segment at `path` (`.<stem>.creating-*`
   beside it). A publisher holds the segment inode's exclusive lock from before
   its hard link until after it deletes its temp, so any lock holder that still
   finds such a temp naming the same inode knows that publisher stopped in the
   link-to-unlink window. The fs layer removes only same-inode candidates."
  [path]
  (let [slash (str/last-index-of path "/")
        directory (subs path 0 slash)
        name (subs path (inc slash))
        stem (segment-stem-from-filename name)]
    (fn []
      (->> (fs/list-directory directory)
           (filter #(publish-temp-basename? stem %))
           (mapv #(str directory "/" %))))))

(defn- with-segment-lock
  "Hold a no-follow lock on the segment entry itself, so the entry that was
   checked is the inode read or written: a symlink swapped in after any check
   is refused rather than followed outside the directory. A published segment
   (`repair?`) first drops a stale publish temp of its own inode under the lock,
   so an interrupted publish never leaves it permanently multiply linked; any
   other extra link is still refused as :clio.ledger/segment-hardlink."
  [acquire! path repair? f]
  (let [lock (try
               (acquire! path (when repair? {:stale-aliases (stale-publish-aliases path)}))
               (catch #?(:clj Exception :cljs :default) cause
                 (case (:clio/error (ex-data cause))
                   :clio.fs/symlink-entry
                   (fail! :clio.ledger/segment-symlink
                          "Segment entry must not be a symbolic link"
                          {:path path})

                   :clio.fs/multiply-linked-entry
                   (fail! :clio.ledger/segment-hardlink
                          "Segment entry must not be a hard link"
                          {:path path})

                   (throw cause))))]
    (try
      (f lock)
      (finally (fs/release-lock! lock)))))

(defn- reclaim-unpublished-temps!
  "After the final segment exists, drop leftover `.<stem>.creating-*` names for
   `event-id` that no live publisher still holds. Same-inode stale aliases are
   already removed when the final entry is locked with repair; distinct-inode
   orphans from an interrupted pre-publish write are removed only when a
   non-blocking lock succeeds, so a concurrent creator's in-flight temp is left
   alone."
  [directory event-id]
  (let [final (segment-path directory event-id)
        stem (encode-segment-stem event-id)]
    (when (fs/exists? final)
      (let [candidates (filterv #(publish-temp-basename? stem %) (fs/list-directory directory))
            removed (atom false)]
        (doseq [name candidates]
          (let [tmp (str directory "/" name)]
            (when-let [lock (fs/try-acquire-entry-lock! tmp)]
              (try
                (fs/delete-if-exists! tmp)
                (reset! removed true)
                (finally (fs/release-lock! lock))))))
        (when @removed
          (fs/sync-directory! directory))))))

(defn ensure-segment-file!
  "Bring the segment file for `event-id` into being exactly once. A concurrent
   or retried creator that finds it already present is not an error. The segment
   directory must already exist — append never silently creates a replacement
   for a typo, deletion, or unmounted volume.

   Prefer `create-segment!` for writes: it publishes a populated segment under
   lock rather than exposing an empty unlocked entry that canonicalization
   could observe as `:clio.ledger/empty-segment`."
  [directory event-id]
  (require-directory! directory)
  (let [path (segment-path directory event-id)]
    (refuse-segment-symlink! path)
    (when-not (fs/exists? path)
      (try
        (create-ledger! path)
        (catch #?(:clj Exception :cljs :default) cause
          (refuse-segment-symlink! path)
          ;; Only a concurrent creator's already-exists race is success. A
          ;; post-creation parent sync (or any other) failure must stay visible
          ;; even when the nondurable file entry remains.
          (if (fs/already-exists-error? cause)
            nil
            (throw cause)))))
    path))

(defn- admit-locked-segment!
  [lock path event]
  (let [existing-text (fs/read-locked-text lock)
        existing (parse-ledger-text path existing-text)]
    (cond
      (empty? existing)
      (do (append-record! lock existing-text event) :appended)

      (and (= 1 (count existing)) (= (first existing) event))
      (do (fs/sync-locked! lock) :already-present)

      (= 1 (count existing))
      (fail! :clio.ledger/id-collision
             "Segment already holds a different event"
             {:path path :event/id (:event/id event)})

      :else
      (fail! :clio.ledger/multi-event-segment
             "Segment file must hold exactly one event"
             {:path path :count (count existing)}))))

(defn- claim-lost-to-winner?
  "True when the temp disappeared because a concurrent winner published the
   final segment and reclaimed our unlocked creating temp."
  [cause path]
  (and (fs/missing-path-error? cause) (fs/exists? path)))

(defn- publish-new-segment!
  "Write the event into a hidden temp, then claim the final name with an
   exclusive hard link so readers never observe an unlocked empty segment.
   After the final name is visible, leftover creating temps for this event id
   are swept so interrupted pre-publish writes cannot accumulate.

   The temp is locked immediately after creation. If a concurrent winner
   reclaims it in the tiny create→lock window (or the hard-link races), and
   the final segment exists, the result is ::claim-lost so create-segment!
   admits through the final path instead of surfacing a raw ENOENT."
  [directory path event identity]
  (let [tmp (publish-temp-path directory (:event/id event))]
    (try
      (create-ledger! tmp)
      (let [outcome
            (try
              (with-segment-lock
                fs/acquire-entry-lock! tmp false
                (fn [lock]
                  (append-record! lock "" event)
                  (let [published
                        (try
                          (ensure-bound-directory! directory identity)
                          (fs/hard-link! tmp path)
                          ;; Drop the temp alias first so the published entry returns to one
                          ;; link, then persist both directory changes before acknowledging.
                          ;; A stop inside this window leaves a same-inode temp that the next
                          ;; segment lock holder removes (see stale-publish-aliases).
                          (ensure-bound-directory! directory identity)
                          (fs/delete-if-exists! tmp)
                          (fs/sync-directory! directory)
                          (ensure-bound-directory! directory identity)
                          :appended
                          (catch #?(:clj Exception :cljs :default) cause
                            (fs/delete-if-exists! tmp)
                            (cond
                              (fs/already-exists-error? cause) ::claim-lost
                              (claim-lost-to-winner? cause path) ::claim-lost
                              :else (throw cause))))]
                    ;; A same-user process may have linked the temp after the
                    ;; one-link check; never acknowledge a multiply linked segment.
                    ;; Sweep any same-inode creating aliases first — a concurrent
                    ;; loser's observation window can otherwise race the check.
                    (when (and (= :appended published) (not= 1 (fs/locked-link-count lock)))
                      (doseq [alias ((stale-publish-aliases path))]
                        (fs/delete-if-exists! alias))
                      (fs/sync-directory! directory)
                      (when (not= 1 (fs/locked-link-count lock))
                        (fail! :clio.ledger/segment-hardlink
                               "Segment gained a hard link during publication"
                               {:path path})))
                    published)))
              (catch #?(:clj Exception :cljs :default) cause
                (if (claim-lost-to-winner? cause path)
                  ::claim-lost
                  (throw cause))))]
        (reclaim-unpublished-temps! directory (:event/id event))
        outcome)
      (catch #?(:clj Exception :cljs :default) cause
        (fs/delete-if-exists! tmp)
        (if (claim-lost-to-winner? cause path)
          ::claim-lost
          (throw cause))))))

(defn- admit-existing-segment!
  "Admit against a final segment that already exists. A concurrent winner may
   still be inside the hard-link→unlink window (nlink 2); if acquire sees that
   as :segment-hardlink, wait briefly and retry until the winner finishes or
   the deadline elapses. A lasting extra link still fails closed."
  [path event]
  (let [deadline (+ (fs/now-ms) 10000)]
    (loop []
      (let [outcome
            (try
              (with-segment-lock
                fs/acquire-entry-lock! path true
                #(admit-locked-segment! % path event))
              (catch #?(:clj Exception :cljs :default) cause
                (if (and (= :clio.ledger/segment-hardlink
                            (:clio/error (ex-data cause)))
                         (< (fs/now-ms) deadline))
                  ::retry
                  (throw cause))))]
        (if (= ::retry outcome)
          (do (fs/sleep-ms! 5) (recur))
          outcome)))))

(defn create-segment!
  "Write one validated event as its own segment. The segment is named by the
   event id, so an exact retry is :already-present and a different event with
   the same id is an :id-collision; other segments are never touched. Unlike
   ordinary ledger append, a preexisting segment never admits a second event.
   A brand-new segment is written to a hidden temp and published by exclusive
   hard link under the writer lock, so concurrent canonicalization never sees
   an unlocked empty entry. An exact retry repeats both durability fences
   before it is acknowledged. The directory is resolved once up front so a
   symlink retarget cannot split publication across two stores."
  [revisions directory event]
  (schema/validate-event! revisions event)
  (let [[directory identity] (bind-segment-directory! directory)
        path (segment-path directory (:event/id event))]
    (ensure-bound-directory! directory identity)
    (refuse-segment-symlink! path)
    (let [outcome
          (try
            (if (fs/exists? path)
              (admit-existing-segment! path event)
              (do (ensure-bound-directory! directory identity)
                  (let [published (publish-new-segment! directory path event identity)]
                    (if (= ::claim-lost published)
                      (admit-existing-segment! path event)
                      published))))
            (catch #?(:clj Exception :cljs :default) cause
              ;; A concurrent winner may still hold a second link in the
              ;; publish window, or our own post-publish check may observe it.
              ;; When the final name exists, admit against it instead of
              ;; surfacing a transient :segment-hardlink to the caller.
              (if (and (= :clio.ledger/segment-hardlink
                          (:clio/error (ex-data cause)))
                       (fs/exists? path))
                (admit-existing-segment! path event)
                (throw cause))))]
      ;; publish-new-segment! already sweeps after a fresh claim; retries that
      ;; find the final name still clear any leftover creating temps.
      (ensure-bound-directory! directory identity)
      (reclaim-unpublished-temps! directory (:event/id event))
      outcome)))

(defn- read-segment!
  "Capture one segment snapshot through a no-follow shared lock and validate it
   as one non-empty `*.edn` file holding exactly the event its name declares.
   The returned snapshot is the one later canonicalized; it is never reread."
  [path]
  (refuse-segment-symlink! path)
  (let [name (last (str/split path #"/"))
        stem (segment-stem-from-filename name)
        expected-id (some-> stem decode-segment-stem)
        ;; Literal uppercase UUID filenames decode, but are not the canonical
        ;; encode-segment-stem form (`!a` escapes). Reject them so they cannot
        ;; alias the lowercase path on case-folding filesystems.
        _ (when (and expected-id
                     (not= name (str (encode-segment-stem expected-id) segment-suffix)))
            (fail! :clio.ledger/noncanonical-segment-name
                   "Segment filename must use the canonical id encoding"
                   {:path path :event/id expected-id}))
        events (with-segment-lock
                 fs/acquire-entry-read-lock! path true
                 #(parse-ledger-text path (fs/read-locked-text %)))]
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
             {:path path :event/id (:event/id (first events))}))
    events))

(defn read-directory
  "Capture validated snapshots of the extra ledger files (for example a frozen
   legacy ledger) and of every segment in `directory`. Each segment snapshot is
   validated for the one-event-per-`<event-id>.edn` format and is the exact
   value returned; callers canonicalize these snapshots without rereading."
  [directory extra-paths]
  (let [paths (segment-paths directory)
        extra (read-ledgers extra-paths)
        snapshots (into extra (map read-segment!) paths)]
    ;; Stale-alias repair may have unlinked a publish temp whose directory
    ;; force failed; a retry no longer sees that alias, so fence the (real)
    ;; segment directory once before returning any snapshot.
    (when-let [first-path (first paths)]
      (fs/sync-directory! (subs first-path 0 (str/last-index-of first-path "/"))))
    snapshots))

(defn canonicalize-directory
  "Canonicalize the extra ledger files together with every segment in
   `directory`, from the validated snapshots captured by read-directory."
  [revisions directory extra-paths]
  (canonicalize/canonicalize revisions (read-directory directory extra-paths)))
