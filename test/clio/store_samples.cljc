(ns clio.store-samples
  "Events and preservation assertions shared by the store tests.

   Insertion order is intentionally not export order. A correct adapter
   sorts by stream id and then `:event/seq`."
  (:require [clio.domain.event-hash :as event-hash]
            [clio.domain.projection :as projection]
            [clio.domain.store :as store]
            #?(:clj [clojure.test :refer [is]]
               :cljs [cljs.test :refer [is]])))

(defn event-id
  [n]
  (str "11111111-1111-4111-8111-11111111111"
       (nth "0123456789abcdef" n)))

(defn- base
  [id stream seq-no causes extra]
  (merge {:event/id id
          :event/stream stream
          :event/seq seq-no
          :event/causes (vec causes)
          :event/type :note/recorded
          :event/actor "user:alice"
          :event/at "2026-08-09T00:00:00.000Z"
          :event/data {:text "plain"}}
         extra))

(defn rich-events
  "One backwards-compatible event, one sealed event, two streams, and the
   portable values the codec has to survive."
  [hash-fn]
  (let [a (event-id 1)
        b (event-id 2)
        c (event-id 3)
        d (event-id 4)
        post (base a "post:p1" 1 []
                   {:event/subject "post:p1"
                    :event/data {:post/id "p1"
                                 :text "hello \u03bb"
                                 :flags #{:a :b/c}
                                 :n 1
                                 :x 0.1
                                 :ok true
                                 :nested {:k [:x "a\nb"]}}})
        post-2 (base b "post:p1" 2 [a]
                     {:event/at #inst "2026-08-09T00:00:00.000Z"
                      :event/body {:id #uuid "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
                                   :at #inst "1969-12-31T23:59:59.999Z"
                                   :syms ['ledger/head]
                                   :empty #{}}})
        box (base c "box:q" 1 [a]
                  {:event/actor "agent:clio"
                   :event/data {:ref a}})
        box-2 (event-hash/seal
               (base d "box:q" 2 [c]
                     {:event/data {:n -3 :zs #{0 1.5}}})
               hash-fn)]
    [box post box-2 post-2]))

(defn plain-events
  [hash-fn]
  (let [a (event-id 5)
        b (event-id 6)
        c (event-id 7)]
    [(base b "s" 2 [a] {:event/data {:n 2}})
     (event-hash/seal (base a "s" 1 [] {:event/data {:n 1}}) hash-fn)
     (base c "t" 1 [] {:event/data {:n 3}})]))

(defn gapped-events
  "A hole in `:event/seq`. The store keeps it; canonicalization would not."
  []
  [(base (event-id 8) "gap" 1 [] {:event/data {:n 1}})
   (base (event-id 9) "gap" 3 [(event-id 8)] {:event/data {:n 3}})])

(defn assert-preserved
  [source target hash-fn]
  (let [source (store/export-events source)
        target (store/export-events target)]
    (is (= source target) "transfer keeps the events")
    (is (= (mapv :event/causes source) (mapv :event/causes target))
        "transfer keeps causes")
    (is (= (store/streams source) (store/streams target)))
    (doseq [stream (store/streams source)]
      (is (= (mapv :event/id (store/read-range source stream {}))
             (mapv :event/id (store/read-range target stream {})))
          "transfer keeps stream order"))
    (is (= (store/head-hash source hash-fn)
           (store/head-hash target hash-fn))
        "transfer keeps the head hash")
    (doseq [event source
            :let [copied (some #(when (= (:event/id %) (:event/id event)) %) target)]]
      (is (= (event-hash/event-hash event hash-fn)
             (event-hash/event-hash copied hash-fn))
          "transfer keeps each event hash"))
    (let [step (fn [counts event]
                 (update counts (:event/stream event) (fnil inc 0)))]
      (is (= (projection/state {:canonical/events source} {} step)
             (projection/state {:canonical/events target} {} step))
          "a fold of the transferred ledger matches the source"))))
