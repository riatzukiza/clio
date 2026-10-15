(ns clio.infra.runtime
  (:require [clio.domain.canonicalize :as canonicalize]
            [clio.domain.schema :as schema]
            #?(:clj [clio.extern.jvm.crypto :as crypto]
               :cljs [clio.extern.js.crypto :as crypto])
            #?(:clj [clio.extern.jvm.fs :as fs]
               :cljs [clio.extern.js.fs :as fs])
            [clio.infra.event :as event]
            [clio.infra.ledger :as ledger]
            [clio.infra.schema-store :as schema-store]
            [clio.law.append :as append-law]
            [clojure.string :as str]))

(defn open
  "Materialize the current source catalog, persist it if it is new, and load all
   historical catalog snapshots. Editing schema data is enough to create a new
   version; there is no version constant to remember."
  [schema-directory catalog]
  (let [current (schema/materialize crypto/sha256 catalog)]
    (schema-store/ensure-revision! schema-directory current)
    {:schema/directory schema-directory
     :schema/current current
     :schema/revisions (schema-store/load-revisions schema-directory)}))

(defn refresh
  [runtime]
  (assoc runtime
         :schema/revisions
         (schema-store/load-revisions (:schema/directory runtime))))

(defn canonicalize-files
  "Capture complete locked snapshots before loading their published schema revisions.
   Schema files are immutable and precede the events that reference them; refreshing
   after capture avoids mixing newer events with an earlier revision inventory."
  [runtime paths]
  (let [snapshots (ledger/read-ledgers paths)
        revisions (:schema/revisions (refresh runtime))]
    (canonicalize/canonicalize revisions snapshots)))

(defn ensure-durable!
  "Refresh schema revisions under the existing ledger's durability lock."
  [runtime path]
  (ledger/ensure-durable-with! #(:schema/revisions (refresh runtime)) path))

(defn retry-append!
  "Explicitly retry a preserved candidate without generating identity or time.
   Exact visible events still require validated inode and parent durability."
  [runtime recovery]
  (append-law/validate-recovery! recovery)
  (try
    (let [runtime (refresh runtime)
          {:ledger/keys [path segment-directory] :keys [event]} recovery
          revisions (:schema/revisions runtime)]
      {:append/result (if segment-directory
                        (do (when-not (= path (ledger/segment-path segment-directory (:event/id event)))
                              (throw (ex-info "Segment recovery must name the event's own segment"
                                              {:clio/error :clio.runtime/invalid-append-recovery
                                               :ledger/path path
                                               :ledger/segment-directory segment-directory})))
                            (ledger/create-segment! revisions segment-directory event))
                        (ledger/append-event! revisions path event))
       :event event
       :runtime runtime})
    (catch #?(:clj Exception :cljs :default) cause
      (throw (ex-info (ex-message cause)
                      (assoc (ex-data cause) :clio/append-recovery recovery)
                      cause)))))

(defn- recovery-path [path]
  (try (fs/absolute-path path)
       (catch #?(:clj Exception :cljs :default) cause
         (if (fs/missing-path-error? cause)
           (throw (ex-info "Ledger file does not exist"
                           {:clio/error :clio.ledger/missing-file :path path} cause))
           (throw cause)))))

(defn append!
  "Create and append an event. Admission/persistence failures expose the exact
   candidate in exception data under :clio/append-recovery for explicit retry."
  [runtime ledger-file schema-id event-data]
  (retry-append! runtime
                 {:ledger/path (recovery-path ledger-file)
                  :event (event/make-event (:schema/current runtime) schema-id event-data)}))

(defn canonicalize-directory
  "Canonicalize extra ledger files plus every segment in `directory`. Segment
   shape is validated while capturing snapshots (empty, multi-event and
   filename/event-id mismatches fail closed); schema revisions are refreshed
   after capture, as in canonicalize-files."
  [runtime directory extra-paths]
  (let [snapshots (ledger/read-directory directory extra-paths)
        revisions (:schema/revisions (refresh runtime))]
    (canonicalize/canonicalize revisions snapshots)))

(defn- recovery-directory
  "Anchor recovery to the directory's real path when it exists, so a symlink
   retarget before retry-append! cannot redirect the write. Fall back to an
   absolute-lexical spelling only when resolution fails because the path is
   confirmed missing (for example the directory vanished between the caller
   check and recovery construction). Any other failure (EACCES, a symlink
   loop, an I/O error) is rethrown before an event exists, since a lexical
   symlink spelling could later resolve to a different store."
  [directory]
  (try
    (fs/absolute-path directory)
    (catch #?(:clj Exception :cljs :default) cause
      (if (fs/missing-path-error? cause)
        (fs/absolute-lexical-path directory)
        (throw cause)))))

(defn append-segment!
  "Create one event and write it as its own segment file in `directory`.
   Failures carry :clio/append-recovery for `retry-append!` like `append!`; the
   recovery names the segment directory, so a retry keeps segment-only admission.
   Segment setup (missing directory, rejected symlink/hard link, and similar)
   runs inside the recovery-wrapping path so callers always receive the
   already-generated event. When `directory` exists, recovery uses its real path
   so a symlink retarget cannot redirect a retry; lexical absolute spelling is
   reserved for a confirmed-missing directory; other resolution failures throw
   before an event is generated."
  [runtime directory schema-id event-data]
  ;; Refuse before any event exists: resolving "" would name the working
  ;; directory, and there is nothing to recover yet.
  (when (or (not (string? directory)) (str/blank? directory))
    (throw (ex-info "Segment directory path must not be empty"
                    {:clio/error :clio.ledger/empty-directory-path :path directory})))
  (let [directory (recovery-directory directory)
        event (event/make-event (:schema/current runtime) schema-id event-data)
        ;; Path is lexical until create-segment! publishes; do not create an
        ;; empty unlocked segment here — that raced with canonicalization.
        recovery {:ledger/path (ledger/segment-path directory (:event/id event))
                  :ledger/segment-directory directory
                  :event event}]
    (try
      (retry-append! runtime recovery)
      (catch #?(:clj Exception :cljs :default) cause
        (if (contains? (ex-data cause) :clio/append-recovery)
          (throw cause)
          (throw (ex-info (ex-message cause)
                          (assoc (or (ex-data cause) {})
                                 :clio/append-recovery recovery)
                          cause)))))))
