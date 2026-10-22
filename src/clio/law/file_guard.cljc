(ns clio.law.file-guard
  "Runtime descriptor-identity guards; never event or catalog data.")

(defn validate-identity!
  [identity]
  (when-not (and (vector? identity) (= 2 (count identity))
                 (every? #(and (string? %) (re-matches #"^(?:0|[1-9][0-9]*)$" %)) identity))
    (throw (ex-info "File identity requires exact decimal device and inode strings"
                    {:clio/error :clio.fs/invalid-file-identity
                     :expected-file-identity identity})))
  identity)

(defn validate-options!
  [options]
  (when-not (or (nil? options)
                (and (map? options) (every? #{:expected-file-identity} (keys options))))
    (throw (ex-info "Unknown file identity guard options"
                    {:clio/error :clio.fs/invalid-file-guard-options})))
  (when (and (some? options) (not (contains? options :expected-file-identity)))
    (throw (ex-info "Explicit guard options must declare an expected file identity"
                    {:clio/error :clio.fs/missing-file-identity})))
  (when (contains? options :expected-file-identity)
    (validate-identity! (:expected-file-identity options)))
  options)

(defn validate-read-options!
  [options]
  (when-not (or (nil? options)
                (and (map? options)
                     (every? #{:expected-file-identities} (keys options))))
    (throw (ex-info "Canonical read requires per-path identity declarations"
                    {:clio/error :clio.fs/invalid-file-guard-options})))
  (when (and (some? options) (not (contains? options :expected-file-identities)))
    (throw (ex-info "Explicit canonical guards must declare per-path identities"
                    {:clio/error :clio.fs/missing-file-identity})))
  (when (contains? options :expected-file-identities)
    (when-not (map? (:expected-file-identities options))
      (throw (ex-info "Canonical identities must be a path-to-identity map"
                      {:clio/error :clio.fs/invalid-file-guard-options})))
    (doseq [identity (vals (:expected-file-identities options))]
      (validate-identity! identity)))
  options)

(defn for-path
  "Select an explicit guard for every declared canonical-read partition."
  [options path]
  (validate-read-options! options)
  (when (contains? options :expected-file-identities)
    (when-not (contains? (:expected-file-identities options) path)
      (throw (ex-info "Canonical partition has no declared file identity"
                      {:clio/error :clio.fs/missing-file-identity :path path})))
    (validate-options! {:expected-file-identity (get (:expected-file-identities options) path)})))
