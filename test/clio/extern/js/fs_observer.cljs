(ns clio.extern.js.fs-observer
  "Observe actual Node calls while retaining the real filesystem and file descriptors."
  (:require ["node:child_process" :as child-process]
            ["node:fs" :as module-fs]
            ["node:path" :as node-path]
            ["node:module" :refer [syncBuiltinESMExports]]))

(def ^:private native (or (.-default module-fs) module-fs))

(defn symbolic-link! [target link]
  (.symlinkSync native target link))

(defn link-count
  "The directory entry's own hard-link count (lstat, so a symlink is not followed)."
  [path]
  (.-nlink (.lstatSync native path)))

(defn mkfifo! [path]
  (.execFileSync child-process "mkfifo" #js [path])
  path)

(defn relative-to-cwd [path]
  (.relative node-path (.cwd js/process) path))

(defn with-observer
  "Call observe! before each sync or rename; injected failures exercise real boundary sequencing."
  [observe! operation]
  (let [original-open (.-openSync native)
        original-sync (.-fsyncSync native)
        original-rename (.-renameSync native)
        handles (atom {})]
    (set! (.-openSync native)
          (fn [& args]
            (let [fd (.apply original-open native (clj->js args))]
              (swap! handles assoc fd (first args)) fd)))
    (set! (.-fsyncSync native)
          (fn [fd]
            (observe! {:operation :sync :path (get @handles fd)
                       :directory? (.isDirectory (.fstatSync native fd))})
            (original-sync fd)))
    (set! (.-renameSync native)
          (fn [from to]
            (observe! {:operation :rename :from from :to to})
            (original-rename from to)))
    (syncBuiltinESMExports)
    (try (operation)
         (finally
           (set! (.-openSync native) original-open)
           (set! (.-fsyncSync native) original-sync)
           (set! (.-renameSync native) original-rename)
           (syncBuiltinESMExports)))))

(defn with-platform
  "Run operation while process.platform reports platform, then restore the host value."
  [platform operation]
  (let [original (.-platform js/process)
        define! (fn [value]
                  (js/Object.defineProperty js/process "platform"
                                            #js {:value value :configurable true}))]
    (define! platform)
    (try (operation) (finally (define! original)))))

(defn with-stat-only-descriptor-proxies
  "Model a host where fd nodes stat as directories but child traversal refuses."
  [operation]
  (let [original-stat (.-statSync native)
        original-close (.-closeSync native)
        probes (atom 0)
        closes (atom 0)]
    (set! (.-statSync native)
          (fn [& args]
            (if (re-matches #"/(?:proc/self/fd|dev/fd)/[0-9]+/\." (first args))
              (do (swap! probes inc)
                  (throw (js/Error. "Injected descriptor proxy traversal refusal")))
              (.apply original-stat native (clj->js args)))))
    (set! (.-closeSync native)
          (fn [fd] (swap! closes inc) (original-close fd)))
    (syncBuiltinESMExports)
    (try {:result (operation) :probes @probes :closes @closes}
         (finally
           (set! (.-statSync native) original-stat)
           (set! (.-closeSync native) original-close)
           (syncBuiltinESMExports)))))
