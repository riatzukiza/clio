(ns clio.extern.js.file-guard-fixture
  (:require [clio.extern.js.fs :as fs]
            ["node:fs" :as module-fs]
            ["node:module" :refer [syncBuiltinESMExports]]))

(def ^:private native (or (.-default module-fs) module-fs))

(defn path-identity [path]
  (let [stat (.statSync native path #js {:bigint true})]
    [(str (.-dev stat)) (str (.-ino stat))]))

(defn with-descriptor-substitution!
  "Open a substitute inode, then restore the inspected pathname before returning the fd."
  [path substitute moved operation]
  (let [open (.-openSync native) swapped (atom false)]
    (set! (.-openSync native)
          (fn [& args]
            (if (and (= path (first args)) (compare-and-set! swapped false true))
              (do (.renameSync native path moved)
                  (.renameSync native substitute path)
                  (try (.apply open native (clj->js args))
                       (finally (.renameSync native path substitute)
                                (.renameSync native moved path))))
              (.apply open native (clj->js args)))))
    (syncBuiltinESMExports)
    (try {:value (operation) :swapped? @swapped}
         (finally (set! (.-openSync native) open) (syncBuiltinESMExports)))))

(def unavailable-code :clio.fs/file-identity-unavailable)
(defn with-unavailable-identity! [operation]
  (with-redefs [fs/locked-file-identity
                (fn [_]
                  (throw (ex-info "Injected descriptor identity refusal"
                                  {:clio/error :clio.fs/file-identity-unavailable})))]
    (operation)))
