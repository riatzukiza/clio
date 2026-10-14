(ns clio.extern.jvm.fs
  (:refer-clojure :exclude [exists?])
  (:require [clojure.string :as str])
  (:import [java.nio ByteBuffer]
           [java.nio.channels FileChannel FileLock]
           [java.nio.charset StandardCharsets]
           [java.nio.file CopyOption Files LinkOption NoSuchFileException OpenOption Path Paths
            StandardCopyOption StandardOpenOption]
           [java.nio.file.attribute BasicFileAttributes FileAttribute]
           [java.util UUID]
           [java.util.concurrent.locks ReentrantLock]))

(def ^:private ^"[Ljava.nio.file.LinkOption;" no-links (make-array LinkOption 0))
(def ^:private no-attributes (make-array FileAttribute 0))
(def ^:private ^ReentrantLock process-guard (ReentrantLock. true))
(def ^:private active-locks (atom {}))

(defn- nio-path ^Path [path]
  (Paths/get ^String path (make-array String 0)))

(defn absolute-path
  "Resolve the existing target through the filesystem, preserving link/.. semantics."
  [path]
  (str (.toRealPath (nio-path path) no-links)))

(defn absolute-lexical-path
  "Make `path` absolute against the process working directory without requiring
   the target to exist. Unlike absolute-path, this does not consult the
   filesystem or resolve symlinks; it only eliminates relative spelling."
  [path]
  (str (.normalize (.toAbsolutePath (nio-path path)))))

(defn missing-path-error? [cause]
  (instance? NoSuchFileException cause))

(defn already-exists-error? [cause]
  (instance? java.nio.file.FileAlreadyExistsException cause))

(def ^:private ^"[Ljava.nio.file.LinkOption;" nofollow-links
  (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

(defn- file-key
  ([path] (file-key path no-links))
  ([path ^"[Ljava.nio.file.LinkOption;" link-options]
  (let [p (nio-path path)
        ^BasicFileAttributes attrs (Files/readAttributes p BasicFileAttributes link-options)]
    (when-not (.isRegularFile attrs)
      (throw (ex-info "Ledger path is not a regular file"
                      {:path path :clio/error :clio.fs/not-regular-file})))
    ;; A path fallback cannot distinguish hard links. Closing a second alias
    ;; descriptor after an overlapping-lock error would drop the first POSIX
    ;; lock, so an opaque/missing identity must fail closed here.
    (or (.fileKey attrs)
        (throw (ex-info "Filesystem does not expose a stable file identity"
                        {:path path :clio/error :clio.fs/unsupported-file-identity}))))))

(defn- with-process-guard
  [f]
  (.lock process-guard)
  (try (f) (finally (.unlock process-guard))))

(defn- refuse-locked-path!
  [path]
  (let [key (file-key path)]
    (when (some #(= key (:file/key %)) (vals @active-locks))
      (throw (ex-info "Path is locked by this process; use the owning descriptor"
                      {:path path :clio/error :clio.fs/locked-path-read})))))

(defn exists? [path]
  (Files/exists (nio-path path) no-links))

(defn symlink? [path]
  (Files/isSymbolicLink (nio-path path)))

(defn require-directory-sync-support!
  "Refuse, without forcing anything, when `path` is not on the supported Linux
   default POSIX filesystem. Directory FileChannels are not portable Java.
   Returns the absolute directory path."
  [path]
  (let [directory (.toAbsolutePath (nio-path path))
        filesystem (.getFileSystem directory)]
    (when-not (and (= "Linux" (System/getProperty "os.name"))
                   (= "file" (.getScheme (.provider filesystem)))
                   (.contains (.supportedFileAttributeViews filesystem) "posix"))
      (throw (ex-info "JVM directory durability is unsupported on this filesystem"
                      {:path path :clio/error :clio.fs/directory-sync-unavailable})))
    directory))

(defn sync-directory!
  "Force directory entries on the supported Linux default POSIX filesystem.

   Directory FileChannels are not portable Java. Refuse other implementations
   and surface open/force failures instead of acknowledging weaker durability."
  [path]
  (let [directory (require-directory-sync-support! path)]
    (try
      (with-open [channel (FileChannel/open directory
                                            (into-array OpenOption [StandardOpenOption/READ]))]
        (.force channel true))
      (catch Exception cause
        (throw (ex-info "JVM directory synchronization failed"
                        {:path path :clio/error :clio.fs/directory-sync-unavailable} cause)))))
  path)

(defn- parent-path [path]
  (str (.getParent (.toAbsolutePath (nio-path path)))))

(defn ensure-dir! [path]
  (let [ancestry (loop [directory (.normalize (.toAbsolutePath (nio-path path))) result []]
                  (if (nil? directory)
                    result
                    (recur (.getParent directory) (conj result (str directory)))))]
    (Files/createDirectories (nio-path path) no-attributes)
    ;; An earlier mkdir may have succeeded before its parent force failed.
    ;; Existing paths therefore need the same durability check on retry.
    (doseq [directory ancestry]
      (sync-directory! directory)))
  path)

(defn force-file!
  "Force an owning file descriptor before acknowledging durable file content."
  [^FileChannel channel]
  (.force channel true))

(defn create-exclusive! [path]
  (with-open [channel (FileChannel/open (nio-path path)
                                      (into-array OpenOption [StandardOpenOption/CREATE_NEW
                                                              StandardOpenOption/WRITE]))]
    (force-file! channel))
  (sync-directory! (parent-path path))
  path)

(defn read-text
  "Path reads cannot close an alias descriptor and silently drop a POSIX lock."
  [path]
  (with-process-guard
    #(do (refuse-locked-path! path)
         (Files/readString (nio-path path) StandardCharsets/UTF_8))))

(defn- write-buffer!
  [^FileChannel channel text]
  (let [buffer (ByteBuffer/wrap (.getBytes ^String text StandardCharsets/UTF_8))]
    (while (.hasRemaining buffer) (.write channel buffer))
    (force-file! channel)))

(defn write-text! [path text]
  (with-process-guard
    #(do
       (when (exists? path) (refuse-locked-path! path))
       (with-open [channel (FileChannel/open
                            (nio-path path)
                            (into-array OpenOption
                                        [StandardOpenOption/CREATE
                                         StandardOpenOption/WRITE
                                         StandardOpenOption/TRUNCATE_EXISTING]))]
         (write-buffer! channel text))
       (sync-directory! (parent-path path))))
  path)

(defn rename! [from to]
  (let [parents (distinct [(parent-path from) (parent-path to)])]
    ;; Check support before changing the namespace, then force both affected
    ;; parents after the atomic move. Post-move failure remains a failed write.
    (doseq [directory parents] (sync-directory! directory))
    (Files/move (nio-path from) (nio-path to)
                (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                                        StandardCopyOption/REPLACE_EXISTING]))
    (doseq [directory parents] (sync-directory! directory)))
  to)

(defn hard-link! [from to]
  (Files/createLink (nio-path to) (nio-path from))
  to)

(defn delete-if-exists! [path]
  (Files/deleteIfExists (nio-path path))
  path)

(defn remove-tree! [path]
  (when (exists? path)
    (with-open [paths (Files/walk (nio-path path)
                                 (make-array java.nio.file.FileVisitOption 0))]
      (doseq [p (reverse (sort-by #(.getNameCount ^Path %)
                                 (iterator-seq (.iterator paths))))]
        (Files/deleteIfExists p))))
  path)

(defn list-files [path]
  (if (exists? path)
    (with-open [paths (Files/list (nio-path path))]
      (mapv #(str (.getFileName ^Path %)) (iterator-seq (.iterator paths))))
    []))

(defn list-directory
  "List entry names in an existing directory. Unlike list-files, a missing or
   vanished directory throws (see missing-path-error?) instead of reading as empty."
  [path]
  (with-open [paths (Files/list (nio-path path))]
    (mapv #(str (.getFileName ^Path %)) (iterator-seq (.iterator paths)))))

(defn- entry-path
  "Resolve only the parent directory, keeping the final entry name unresolved so
   a no-follow open binds to the directory entry itself."
  [path]
  (let [p (.toAbsolutePath (nio-path path))]
    (str (.resolve (.toRealPath (.getParent p) no-links) (.getFileName p)))))

(defn- symlink-entry-error? [^Throwable cause]
  ;; Linux reports O_NOFOLLOW on a symlink as ELOOP.
  (and (instance? java.nio.file.FileSystemException cause)
       (some-> (.getReason ^java.nio.file.FileSystemException cause)
               (str/includes? "symbolic links"))))

(defn- link-count
  [path target-path links]
  (try (long (Files/getAttribute (nio-path target-path) "unix:nlink" links))
       (catch Exception cause
         (throw (ex-info "Filesystem does not expose link count"
                         {:path path
                          :clio/error :clio.fs/unsupported-file-identity}
                         cause)))))

(defn- same-inode-entry?
  "Read `candidate` without following links: true when it is a regular file
   with the file identity `key`. A candidate that vanished is not an alias."
  [candidate key]
  (try
    (let [^BasicFileAttributes attrs (Files/readAttributes (nio-path candidate)
                                                           BasicFileAttributes
                                                           nofollow-links)]
      (and (.isRegularFile attrs) (= key (.fileKey attrs))))
    (catch NoSuchFileException _ false)))

(defn- repair-stale-aliases!
  "With the inode lock held and more than one link observed, delete each
   caller-named candidate entry that is this same inode, then force the parent
   directory. The caller names only entries whose owner must hold this lock
   while they exist, so a lock holder that still sees them knows the owner is
   gone. Anything else keeps its link and the link-count guard still refuses."
  [target-path key stale-aliases]
  (let [aliases (filterv #(same-inode-entry? % key) (stale-aliases))]
    (doseq [alias aliases] (Files/deleteIfExists (nio-path alias)))
    (when (seq aliases) (sync-directory! (parent-path target-path)))))

(defn- acquire-lock-mode!
  [path read-only? nofollow? {:keys [stale-aliases]}]
  (.lock process-guard)
  (try
    ;; Resolve before opening so a later symlink retarget cannot redirect the
    ;; parent fence away from the inode held by this channel. A no-follow open
    ;; instead binds the named entry and refuses a symlink there.
    (let [target-path (if nofollow? (entry-path path) (absolute-path path))
          links (if nofollow? nofollow-links no-links)
          key (try (file-key target-path links)
                   (catch clojure.lang.ExceptionInfo cause
                     (if (and nofollow? (Files/isSymbolicLink (nio-path target-path)))
                       (throw (ex-info "Directory entry is a symbolic link"
                                       {:path path :clio/error :clio.fs/symlink-entry} cause))
                       (throw cause))))
          _ (when (some #(= key (:file/key %)) (vals @active-locks))
              (throw (ex-info "Path is locked by this process; use the owning descriptor"
                              {:path target-path :clio/error :clio.fs/locked-path-read})))
          options (cond-> (if read-only? [StandardOpenOption/READ]
                              [StandardOpenOption/READ StandardOpenOption/WRITE])
                    nofollow? (conj LinkOption/NOFOLLOW_LINKS))
          channel (try (FileChannel/open (nio-path target-path)
                                         ^"[Ljava.nio.file.OpenOption;" (into-array OpenOption options))
                       (catch Exception cause
                         (if (and nofollow? (symlink-entry-error? cause))
                           (throw (ex-info "Directory entry is a symbolic link"
                                           {:path path :clio/error :clio.fs/symlink-entry} cause))
                           (throw cause))))
          ;; Re-read the directory entry after open. If it was replaced between
          ;; file-key and open, the path now names a different inode than `key`
          ;; (the one we intended); refuse rather than locking the substitute.
          opened-key (try (file-key target-path links)
                          (catch Exception cause
                            (.close channel)
                            (throw cause)))]
      (when-not (= key opened-key)
        (.close channel)
        (throw (ex-info "Directory entry changed between inspect and open"
                        {:path path :clio/error :clio.fs/entry-replaced})))
      (try
        (let [lock (.lock channel 0 Long/MAX_VALUE (boolean read-only?))
              token (str (UUID/randomUUID))]
          (when nofollow?
            ;; Link-count and stale-alias repair run only after the entry still
            ;; names the inspected inode, so they describe the opened file.
            (when-not (= key (file-key target-path links))
              (throw (ex-info "Directory entry changed after open"
                              {:path path :clio/error :clio.fs/entry-replaced})))
            (when (and stale-aliases (< 1 (link-count path target-path links)))
              (repair-stale-aliases! target-path key stale-aliases))
            (when-not (= key (file-key target-path links))
              (throw (ex-info "Directory entry changed during stale-alias repair"
                              {:path path :clio/error :clio.fs/entry-replaced})))
            (let [nlink (link-count path target-path links)]
              (when-not (= 1 nlink)
                (throw (ex-info "Directory entry has additional hard links"
                                {:path path
                                 :nlink nlink
                                 :clio/error :clio.fs/multiply-linked-entry})))))
          (swap! active-locks assoc token
                 {:channel channel :file-lock lock :file/key key
                  :path path :target-path target-path})
          {:lock/id token :lock/path path})
        (catch Throwable cause
          (.close channel)
          (throw cause))))
    (catch Throwable cause
      (.unlock process-guard)
      (throw cause))))

(defn acquire-lock!
  "Lock an existing ledger inode using FileChannel's exclusive whole-file lock.

   JVM FileChannel uses POSIX record locking on Unix and interoperates with
   Clio's Node fcntl lock. A process guard serializes acquisition before opening
   any second descriptor: closing even a failed overlapping channel could drop
   the process's existing POSIX lock. This deliberately serializes local JVM
   ledger operations; separate processes still coordinate per inode in the OS.
   The returned Clojure token carries no host channel or lock object."
  [path]
  (acquire-lock-mode! path false false nil))

(defn acquire-read-lock!
  "Hold a shared whole-file lock using a genuinely read-only FileChannel.

   Shared readers conflict with Clio's exclusive POSIX writers. They retain the
   same local inode guard and release path, and cannot create or append files."
  [path]
  (acquire-lock-mode! path true false nil))

(defn acquire-entry-lock!
  "Like acquire-lock!, but open the directory entry with NOFOLLOW_LINKS so the
   checked entry is the opened inode: a symlink swapped in after a check fails
   with :clio.fs/symlink-entry instead of redirecting outside the directory.

   The entry must have exactly one link once locked. `:stale-aliases`, when
   given, is a zero-argument function naming candidate entries that may only
   exist while their owner holds this inode's lock (an interrupted publisher's
   temp). Under the lock, candidates that are this same inode are deleted and
   the directory is forced before the one-link guard runs."
  ([path] (acquire-entry-lock! path nil))
  ([path options] (acquire-lock-mode! path false true options)))

(defn try-acquire-entry-lock!
  "Non-blocking counterpart of acquire-entry-lock!. Returns nil when the entry
   is missing, locked by another holder, multiply linked, or unavailable."
  [path]
  (.lock process-guard)
  (try
    (let [target-path (entry-path path)
          links nofollow-links]
      (when (Files/exists (nio-path target-path) nofollow-links)
        (let [key (file-key target-path links)]
          (when-not (some #(= key (:file/key %)) (vals @active-locks))
            (let [options (into-array OpenOption [StandardOpenOption/READ
                                                  StandardOpenOption/WRITE
                                                  LinkOption/NOFOLLOW_LINKS])
                  channel (FileChannel/open (nio-path target-path) options)]
              (try
                (let [lock (.tryLock channel 0 Long/MAX_VALUE false)]
                  (if (nil? lock)
                    (do (.close channel) nil)
                    (if (not= 1 (link-count path target-path links))
                      (do (.release lock) (.close channel) nil)
                      (let [token (str (UUID/randomUUID))]
                        (swap! active-locks assoc token
                               {:channel channel :file-lock lock :file/key key
                                :path path :target-path target-path})
                        {:lock/id token :lock/path path}))))
                (catch Throwable cause
                  (.close channel)
                  (throw cause))))))))
    (catch Exception _
      nil)
    (finally
      ;; release-lock! unlocks process-guard; if we return a lock, keep it held.
      (let [held (some (fn [[_ v]] (= path (:path v))) @active-locks)]
        (when-not held
          (.unlock process-guard))))))

(defn acquire-entry-read-lock!
  "Shared-lock counterpart of acquire-entry-lock!, with the same `:stale-aliases`."
  ([path] (acquire-entry-read-lock! path nil))
  ([path options] (acquire-lock-mode! path true true options)))

(defn- lock-entry [token]
  (or (get @active-locks (:lock/id token))
      (throw (ex-info "Ledger lock token is not active"
                      {:clio/error :clio.fs/inactive-lock}))))

(defn read-locked-text
  "Read only through the descriptor that owns the inode lock."
  [token]
  (let [{:keys [^FileChannel channel]} (lock-entry token)
        size (.size channel)]
    (when (> size Integer/MAX_VALUE)
      (throw (ex-info "Ledger exceeds the in-memory reader limit"
                      {:clio/error :clio.fs/ledger-too-large :bytes size})))
    (.position channel 0)
    (let [buffer (ByteBuffer/allocate (int size))]
      (loop []
        (when (and (.hasRemaining buffer) (not= -1 (.read channel buffer)))
          (recur)))
      (.flip buffer)
      (str (.decode (.newDecoder StandardCharsets/UTF_8) buffer)))))

(defn append-locked-text!
  "Append and force the owning inode, then its directory entry, before acknowledgment."
  [token text]
  (let [{:keys [^FileChannel channel path target-path]} (lock-entry token)]
    ;; Refuse an unsupported host before the event becomes visible, as rename!
    ;; checks before moving, so the append fails without writing an event that
    ;; every retry would then report as failed. Transient force failures after
    ;; the write keep the uncertain-write retry semantics.
    (require-directory-sync-support! (parent-path target-path))
    (.position channel (.size channel))
    (write-buffer! channel text)
    (sync-directory! (parent-path target-path))
    path))

(defn sync-locked!
  "Reflush the locked inode and parent, including an uncertain earlier creation."
  [token]
  (let [{:keys [channel path target-path]} (lock-entry token)]
    (force-file! channel)
    (sync-directory! (parent-path target-path))
    path))

(defn release-lock!
  [token]
  (let [{:keys [^FileChannel channel ^FileLock file-lock]} (lock-entry token)]
    (try
      (.release file-lock)
      (finally
        (try (.close channel)
             (finally
               (swap! active-locks dissoc (:lock/id token))
               (.unlock process-guard))))))
  nil)
