(ns clio.shape.time
  "Portable UTC instants, as text, without a host calendar.

   `#inst` printing disagrees across hosts before the Gregorian cutover, so
   this namespace owns the epoch-millisecond conversion used by the JSON
   codec. The EDN reader is still the constructor: a millisecond value is
   admitted only when the text we format reads back as that same millisecond."
  (:require [clio.shape.edn :as edn]))

(def ^:private ms-per-day 86400000)

(defn- div-floor
  [n d]
  (let [q (quot n d)]
    (if (neg? (rem n d)) (dec q) q)))

(defn- mod-floor
  [n d]
  (let [r (rem n d)]
    (if (neg? r) (+ r d) r)))

(defn- pad
  [width n]
  (let [text (str n)]
    (if (>= (count text) width)
      text
      (str (apply str (repeat (- width (count text)) "0")) text))))

(defn- civil-from-unix-day
  "Howard Hinnant's civil_from_days. `z` is days since 1970-01-01."
  [z]
  (let [z (+ z 719468)
        era (quot (if (>= z 0) z (- z 146096)) 146097)
        doe (- z (* era 146097))
        yoe (quot (- (+ (- doe (quot doe 1460)) (quot doe 36524))
                     (quot doe 146096))
                  365)
        y (+ yoe (* era 400))
        doy (- doe (- (+ (* 365 yoe) (quot yoe 4)) (quot yoe 100)))
        mp (quot (+ (* 5 doy) 2) 153)
        d (inc (- doy (quot (+ (* 153 mp) 2) 5)))
        m (if (< mp 10) (+ mp 3) (- mp 9))
        y (if (<= m 2) (inc y) y)]
    [y m d]))

(defn millis->inst-text
  "UTC `#inst` body, millisecond precision, always a `Z` suffix."
  [millis]
  (let [day (div-floor millis ms-per-day)
        tod (mod-floor millis ms-per-day)
        [y m d] (civil-from-unix-day day)
        hour (quot tod 3600000)
        minute (quot (mod-floor tod 3600000) 60000)
        second (quot (mod-floor tod 60000) 1000)
        milli (mod-floor tod 1000)]
    (str (pad 4 y) "-" (pad 2 m) "-" (pad 2 d)
         "T" (pad 2 hour) ":" (pad 2 minute) ":" (pad 2 second)
         "." (pad 3 milli) "Z")))

(defn inst-from-millis
  "Build an instant by reading the EDN tag we formatted.

   The read must land on the same millisecond. A calendar bug therefore
   refuses the value instead of shifting it."
  [millis]
  (let [text (millis->inst-text millis)
        value (edn/read-one (str "#inst \"" text "\""))]
    (when-not (= millis (inst-ms value))
      (throw (ex-info "Instant milliseconds do not round-trip through EDN"
                      {:clio/error :clio.json/invalid-instant
                       :millis millis
                       :text text
                       :read (inst-ms value)})))
    value))
