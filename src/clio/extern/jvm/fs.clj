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
(def ^:private pinned-directories (atom {}))

(defn- nio-path ^Path [path]
  (Paths/get ^String path (make-array String 0)))

(defn absolute-path
  "Resolve the existing target through the filesystem, preserving link/.. semantics."
  [path]
  (if (contains? @pinned-directories path)
    path
    (str (.toRealPath (nio-path path) no-links))))

(defn absolute-lexical-path
  "Make `path` absolute against the process working directory without requiring
   the target to exist. Unlike absolute-path, this does not consult the
   filesystem or resolve symlinks; it only eliminates relative spelling."
  [path]
  (str (.normalize (.toAbsolutePath (nio-path path)))))

(defn directory-identity
  "Device/inode of an existing directory after resolving the final path."
  [path]
  (let [p (nio-path path)
        ^BasicFileAttributes attrs (Files/readAttributes p BasicFileAttributes no-links)]
    (when-not (.isDirectory attrs)
      (throw (ex-info "Path is not a directory"
                      {:path path :clio/error :clio.fs/not-directory})))
    [(str (Files/getAttribute p "unix:dev" no-links))
     (str (Files/getAttribute p "unix:ino" no-links))]))

(defn same-directory-identity?
  "True when `path` still names the directory identity captured earlier."
  [path identity]
  (try
    (and (= (directory-identity path) identity)
         (if-let [logical (get @pinned-directories path)]
           (= (directory-identity logical) identity)
           true))
    (catch Exception _ false)))

(defn missing-path-error? [cause]
  (instance? NoSuchFileException cause))

(defn now-ms
  []
  (System/currentTimeMillis))

(defn sleep-ms!
  [ms]
  (Thread/sleep ms))

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

(def ^:private fdinfo-pos-pattern #"(?m)^pos:\s+(\d+)\s*$")

(defn- fdinfo-position
  [fd-name]
  (try
    (some->> (Files/readString (nio-path (str "/proc/self/fdinfo/" fd-name))
                               StandardCharsets/UTF_8)
             (re-find fdinfo-pos-pattern)
             second
             parse-long)
    ;; A descriptor closed during the scan (or an unreadable entry) is not ours.
    (catch Exception _ nil)))

(def ^:private fd-tag-min
  "Lowest fdinfo position tag (2^24): far above the offsets ordinary small
   files reach."
  (bit-shift-left 1 24))

(def ^:private fd-tag-max
  "Exclusive upper bound for the fdinfo position tag (2^31). It stays below
   vfat's 0xffffffff s_maxbytes and ext4's 1 KiB-block indirect limit (~2^34),
   the smallest common Linux limits, where a larger lseek fails with EINVAL."
  (bit-shift-left 1 31))

(defn- channel-fd-int
  "OS file descriptor number owned by an open FileChannel, without reflection.

   The channel's file position is a property of its own open file description,
   so a random position in [fd-tag-min, fd-tag-max) = [2^24, 2^31) tags it
   (below the smallest common s_maxbytes, vfat's 0xffffffff, where lseek would
   fail with EINVAL). A coincident position on another descriptor yields two
   matches and fails closed. /proc/self/fdinfo reports each
   descriptor's position; exactly one descriptor carries the tag. The original
   position is restored. No JVM module opens are needed, so :local/root
   consumers work unchanged. Without Linux /proc the identity check fails closed."
  ^long [^FileChannel channel]
  (let [saved (try (.position channel)
                   (catch java.io.IOException cause
                     ;; ESPIPE: the descriptor is a FIFO/socket, not the inspected file.
                     (throw (ex-info "Opened entry is not a seekable regular file"
                                     {:clio/error :clio.fs/entry-replaced} cause))))
        tag (.nextLong (java.util.concurrent.ThreadLocalRandom/current)
                       (long fd-tag-min) (long fd-tag-max))]
    (try
      (.position channel (long tag))
      (let [matches (try
                      (with-open [entries (Files/list (nio-path "/proc/self/fdinfo"))]
                        (into []
                              (comp (map #(str (.getFileName ^Path %)))
                                    (filter #(= tag (fdinfo-position %))))
                              (iterator-seq (.iterator entries))))
                      (catch Exception _ []))]
        (if (= 1 (count matches))
          (parse-long (first matches))
          (throw (ex-info "Cannot determine the FileChannel descriptor identity"
                          {:clio/error :clio.fs/channel-identity-unavailable
                           :matches (count matches)}))))
      (finally
        (.position channel (long saved))))))

(defn with-pinned-directory!
  "Call f with a path anchored to an owning read-only directory channel.
   Uses the same descriptor discovery as segment-entry locks; unsupported
   binding fails closed instead of resolving back to a replaceable pathname.
   The descriptor path is valid only within f and is closed on every outcome."
  [path f]
  (with-open [channel (FileChannel/open (nio-path path)
                                      (into-array OpenOption [StandardOpenOption/READ]))]
    (let [fd (channel-fd-int channel)
          pinned (str "/proc/self/fd/" fd)]
      (directory-identity pinned)
      (swap! pinned-directories assoc pinned path)
      (try (f pinned)
           (finally (swap! pinned-directories dissoc pinned))))))

(defn- display-entry-path [path]
  (let [p (nio-path path)]
    (if-let [directory (get @pinned-directories (str (.getParent p)))]
      (str (.resolve (nio-path directory) (.getFileName p)))
      path)))

(defn- fd-file-key
  "Identity of the inode owned by descriptor `fd`, via Linux `/proc/self/fd/<n>`.

   Unlike a path re-stat after open, this cannot be fooled by an ABA rename that
   restores the original directory entry while the channel still holds a
   substitute inode."
  [fd]
  (file-key (str "/proc/self/fd/" fd) no-links))

(defn- fd-link-count
  [fd path]
  (try
    (long (Files/getAttribute (nio-path (str "/proc/self/fd/" fd))
                              "unix:nlink"
                              no-links))
    (catch Exception cause
      (throw (ex-info "Filesystem does not expose link count"
                      {:path path
                       :clio/error :clio.fs/unsupported-file-identity}
                      cause)))))

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
    (str (.resolve (nio-path (absolute-path (str (.getParent p)))) (.getFileName p)))))

(defn- symlink-entry-error? [^Throwable cause]
  ;; Linux reports O_NOFOLLOW on a symlink as ELOOP.
  (and (instance? java.nio.file.FileSystemException cause)
       (some-> (.getReason ^java.nio.file.FileSystemException cause)
               (str/includes? "symbolic links"))))

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

(def ^:dynamic *entry-open-timeout-ms*
  "Upper bound for the read-only fallback open of a checked segment entry. A
   regular file opens at once; a FIFO swapped in after the type check would
   block a read-only open (the JVM has no O_NONBLOCK), so the open fails closed."
  5000)

(def ^:dynamic *max-stuck-entry-opens*
  "Abandoned fallback opens still blocked in the kernel. At this many, further
   fallback opens refuse at once instead of starting another thread, so a FIFO
   swap cannot accumulate blocked threads."
  1)

(def ^:private stuck-entry-opens (atom 0))

(defn- open-entry-bounded!
  "Open `target-path` on a daemon worker and wait at most
   *entry-open-timeout-ms*. On timeout the open is abandoned and
   :clio.fs/entry-open-blocked is thrown. A late channel is closed only under
   process-guard, so no lock this process holds can be dropped by that close."
  ^FileChannel [target-path ^"[Ljava.nio.file.OpenOption;" options path]
  (when (<= (long *max-stuck-entry-opens*) (long @stuck-entry-opens))
    (throw (ex-info "An earlier entry open is still blocked; refusing another"
                    {:path path :clio/error :clio.fs/entry-open-blocked
                     :stuck-opens @stuck-entry-opens})))
  (let [state (atom ::waiting)
        worker (doto (Thread.
                      ^Runnable
                      (fn []
                        (let [outcome (try {:channel (FileChannel/open (nio-path target-path) options)}
                                           (catch Throwable cause {:error cause}))]
                          (when-not (compare-and-set! state ::waiting outcome)
                            (swap! stuck-entry-opens dec)
                            (when-let [^FileChannel late (:channel outcome)]
                              (.lock process-guard)
                              (try (.close late) (finally (.unlock process-guard))))))))
                 (.setDaemon true)
                 (.setName "clio-entry-open")
                 (.start))]
    (try
      (.join worker (long *entry-open-timeout-ms*))
      (catch InterruptedException cause
        ;; Same abandonment path as a timeout: count the stuck open so the cap
        ;; still applies, and let the worker close a late channel. If the open
        ;; finished first, close that unclaimed channel here.
        (when-not (compare-and-set! state ::waiting ::abandoned)
          (when-let [^FileChannel early (:channel @state)]
            (.lock process-guard)
            (try (.close early) (finally (.unlock process-guard)))))
        (when (= ::abandoned @state) (swap! stuck-entry-opens inc))
        (.interrupt (Thread/currentThread))
        (throw cause)))
    (if (compare-and-set! state ::waiting ::abandoned)
      (do (swap! stuck-entry-opens inc)
          (throw (ex-info "Directory entry open did not complete; refusing a special-file swap"
                          {:path path :clio/error :clio.fs/entry-open-blocked})))
      (let [{:keys [channel error]} @state]
        (if error (throw error) channel)))))

(defn- file-store-read-only?
  "True when the file store holding `target-path` is mounted read-only. A
   stable, locale-independent signal for EROFS (provider reason text varies)."
  [target-path]
  (try (.isReadOnly (Files/getFileStore (nio-path target-path)))
       (catch Exception _ false)))

(defn- read-only-target-error? [^Throwable cause target-path]
  (or (instance? java.nio.file.AccessDeniedException cause)
      (and (instance? java.nio.file.FileSystemException cause)
           (or (some-> (.getReason ^java.nio.file.FileSystemException cause)
                       (str/includes? "Read-only file system"))
               (file-store-read-only? target-path)))))

(defn- open-entry-read!
  "Open a checked entry for a shared lock without blocking on a FIFO swap.

   Linux never blocks an O_RDWR open of a FIFO, so try READ+WRITE first (the
   lock token stays read-only: append-locked-text! refuses it). Only an entry
   this process may not write (permissions, read-only filesystem) falls back to
   a READ open bounded by open-entry-bounded!."
  ^FileChannel [target-path path]
  (try
    (FileChannel/open (nio-path target-path)
                      ^"[Ljava.nio.file.OpenOption;"
                      (into-array OpenOption [StandardOpenOption/READ StandardOpenOption/WRITE
                                              LinkOption/NOFOLLOW_LINKS]))
    (catch Exception cause
      (if (read-only-target-error? cause target-path)
        (open-entry-bounded! target-path
                             (into-array OpenOption [StandardOpenOption/READ LinkOption/NOFOLLOW_LINKS])
                             path)
        (throw cause)))))

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
          open-options ^"[Ljava.nio.file.OpenOption;" (into-array OpenOption options)
          channel (try (if (and nofollow? read-only?)
                         ;; Read-only opens are the ones a FIFO swap can block.
                         (open-entry-read! target-path path)
                         (FileChannel/open (nio-path target-path) open-options))
                       (catch Exception cause
                         (if (and nofollow? (symlink-entry-error? cause))
                           (throw (ex-info "Directory entry is a symbolic link"
                                           {:path path :clio/error :clio.fs/symlink-entry} cause))
                           (throw cause))))
          ;; Entry locks bind validation to the channel's inode, not a path
          ;; re-stat. An ABA swap can restore the original directory entry after
          ;; open while this channel still owns a substitute inode;
          ;; /proc/self/fd reports the descriptor's identity. Ordinary
          ;; (link-following) ledger locks keep their pre-segment behavior.
          fd (when nofollow?
               (try (channel-fd-int channel)
                    (catch Exception cause
                      (.close channel)
                      (throw cause))))
          opened-key (when nofollow?
                       (try (fd-file-key fd)
                            (catch Exception cause
                              (.close channel)
                              (throw cause))))]
      (when (and nofollow? (not= key opened-key))
        (.close channel)
        (throw (ex-info "Directory entry changed between inspect and open"
                        {:path path :clio/error :clio.fs/entry-replaced})))
      (try
        (let [lock (.lock channel 0 Long/MAX_VALUE (boolean read-only?))
              token (str (UUID/randomUUID))]
          (when nofollow?
            ;; Link-count and stale-alias repair describe the opened inode.
            (when-not (= key (fd-file-key fd))
              (throw (ex-info "Directory entry changed after open"
                              {:path path :clio/error :clio.fs/entry-replaced})))
            (when (and stale-aliases (< 1 (fd-link-count fd path)))
              (repair-stale-aliases! target-path key stale-aliases))
            (when-not (= key (fd-file-key fd))
              (throw (ex-info "Directory entry changed during stale-alias repair"
                              {:path path :clio/error :clio.fs/entry-replaced})))
            (let [nlink (fd-link-count fd path)]
              ;; nlink 0: a concurrent reclaim unlinked the entry after open.
              (when (zero? nlink)
                (throw (ex-info "Directory entry was unlinked after open"
                                {:path path
                                 :nlink nlink
                                 :clio/error :clio.fs/unlinked-entry})))
              (when-not (= 1 nlink)
                (throw (ex-info "Directory entry has additional hard links"
                                {:path path
                                 :nlink nlink
                                 :clio/error :clio.fs/multiply-linked-entry})))))
          (swap! active-locks assoc token
                 {:channel channel :file-lock lock :file/key key
                  :path (display-entry-path path) :target-path target-path :read-only? (boolean read-only?)})
          {:lock/id token :lock/path (display-entry-path path)})
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
  (let [created? (volatile! false)]
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
                  (let [lock (.tryLock channel 0 Long/MAX_VALUE false)
                        ;; Bind to the opened descriptor's inode (/proc/self/fd),
                        ;; like acquire-lock-mode!: an entry swapped between the
                        ;; path stat and the open must not yield a token whose
                        ;; recorded key names a different inode.
                        fd (when lock (channel-fd-int channel))]
                    (if (nil? lock)
                      (do (.close channel) nil)
                      (if (or (not= key (fd-file-key fd))
                              (not= 1 (fd-link-count fd path)))
                        (do (.release lock) (.close channel) nil)
                        (let [token (str (UUID/randomUUID))]
                          (swap! active-locks assoc token
                                 {:channel channel :file-lock lock :file/key key
                                  :path path :target-path target-path})
                          (vreset! created? true)
                          {:lock/id token :lock/path path}))))
                  (catch Throwable cause
                    (.close channel)
                    (throw cause))))))))
      (catch Exception _
        nil)
      (finally
        ;; release-lock! unlocks process-guard once for the token this call
        ;; returns; every other outcome (including a path this thread already
        ;; owns) drops the hold taken above.
        (when-not @created?
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
  (let [{:keys [^FileChannel channel path target-path read-only?]} (lock-entry token)]
    ;; A shared entry lock may own a READ+WRITE channel (FIFO-safe open); it
    ;; still never writes.
    (when read-only? (throw (java.nio.channels.NonWritableChannelException.)))
    ;; Refuse an unsupported host before the event becomes visible, as rename!
    ;; checks before moving, so the append fails without writing an event that
    ;; every retry would then report as failed. Transient force failures after
    ;; the write keep the uncertain-write retry semantics.
    (require-directory-sync-support! (parent-path target-path))
    (.position channel (.size channel))
    (write-buffer! channel text)
    (sync-directory! (parent-path target-path))
    path))

(defn locked-entry-owns-path?
  "True when `path` (no-follow) still names the inode owned by `token`."
  [token path]
  (let [{:keys [file/key]} (lock-entry token)]
    (same-inode-entry? path key)))

(defn delete-locked-entry-if-owns!
  "Unlink `path` only when it still names the locked inode. Returns true when
   the entry was removed (or already absent after a same-inode check)."
  [token path]
  (if-not (locked-entry-owns-path? token path)
    false
    ;; Check and unlink cannot be one syscall on a shared name, so first move
    ;; whatever inode `path` names now to a private quarantine name nobody else
    ;; knows, then verify and unlink only that name. A replacement swapped in
    ;; after the check is moved back with a no-clobber hard link.
    (let [{:keys [file/key]} (lock-entry token)
          quarantine (str path ".reclaim-" (UUID/randomUUID))
          moved? (try (Files/move (nio-path path) (nio-path quarantine)
                                  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))
                      true
                      (catch java.nio.file.NoSuchFileException _ false))]
      (cond
        (not moved?) false
        (same-inode-entry? quarantine key)
        (do (Files/deleteIfExists (nio-path quarantine)) true)
        :else
        (do (try (Files/createLink (nio-path path) (nio-path quarantine))
                 (catch java.nio.file.FileAlreadyExistsException cause
                   (throw (ex-info "Reclaim moved an unrelated entry and cannot restore it"
                                   {:path path :quarantine quarantine
                                    :clio/error :clio.fs/reclaim-restore-conflict}
                                   cause))))
            (Files/deleteIfExists (nio-path quarantine))
            false)))))

(defn locked-link-count
  "Link count of the inode owned by the lock's channel (/proc/self/fd, not a path)."
  [token]
  (let [{:keys [^FileChannel channel path]} (lock-entry token)]
    (fd-link-count (channel-fd-int channel) path)))

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
