(ns clio.extern.jvm.file-guard-fixture
  (:require [clio.extern.jvm.fs :as fs])
  (:import [java.nio.file Files Paths LinkOption CopyOption]))

(defn- nio [path] (Paths/get path (make-array String 0)))
(defn path-identity [path]
  [(str (Files/getAttribute (nio path) "unix:dev" (make-array LinkOption 0)))
   (str (Files/getAttribute (nio path) "unix:ino" (make-array LinkOption 0)))])

(defn with-descriptor-substitution!
  "Swap after the path inspection, restore after actual FileChannel fd discovery."
  [path substitute moved operation]
  (let [file-key @#'fs/file-key discover @#'fs/channel-fd-int swapped (atom false)
        restored (atom false)
        move! #(Files/move (nio %1) (nio %2) (make-array CopyOption 0))
        restore! #(when (and @swapped (compare-and-set! restored false true))
                    (move! path substitute) (move! moved path))]
    (with-redefs [fs/file-key (fn
                               ([p] (file-key p))
                               ([p links]
                                (let [key (file-key p links)]
                                  (when (and (= path p) (compare-and-set! swapped false true))
                                    (move! path moved) (move! substitute path))
                                  key)))
                  fs/channel-fd-int (fn ^long [channel]
                                      (let [fd (discover channel)] (restore!) fd))]
      (try {:value (operation) :swapped? @swapped}
           (finally (restore!))))))
