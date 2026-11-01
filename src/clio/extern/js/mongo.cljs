(ns clio.extern.js.mongo
  "Node MongoDB driver, blocked into the synchronous ledger protocol.

   The driver is async and is loaded only when a store is opened. It runs
   on a worker thread; the caller waits with Atomics so `append` stays a
   plain function, matching the file adapters. The worker is the only place
   that touches the driver. Missing driver and unreachable servers are
   refusals, not empty ledgers."
  (:require ["node:path" :as node-path]
            ["node:worker_threads" :as worker-threads]
            [clojure.string :as str]))

(def ^:private buffer-bytes (* 8 1024 1024))

(def ^:private worker-source
  (str/join "\n"
            ["'use strict';"
             "var workerThreads = require('node:worker_threads');"
             "var moduleApi = require('node:module');"
             "var workerData = workerThreads.workerData;"
             "var header = new Int32Array(workerData.buffer, 0, 4);"
             "var bytes = new Uint8Array(workerData.buffer);"
             "function write(obj) {"
             "  var text = JSON.stringify(obj);"
             "  var encoded = Buffer.from(text, 'utf8');"
             "  if (encoded.length + 16 > bytes.length) {"
             "    encoded = Buffer.from(JSON.stringify({err: 'driver', message: 'response too large'}));"
             "  }"
             "  bytes.set(encoded, 16);"
             "  Atomics.store(header, 1, encoded.length);"
             "}"
             "function read() {"
             "  var len = Atomics.load(header, 1);"
             "  return JSON.parse(Buffer.from(bytes.subarray(16, 16 + len)).toString('utf8'));"
             "}"
             "async function waitFor(target) {"
             "  for (;;) {"
             "    var cur = Atomics.load(header, 0);"
             "    if (cur === target) return;"
             "    var waiting = Atomics.waitAsync(header, 0, cur);"
             "    if (waiting.async) await waiting.value;"
             "  }"
             "}"
             "function failBoot(err, message) {"
             "  write({err: err, message: message});"
             "  Atomics.store(header, 0, 9);"
             "  Atomics.notify(header, 0, 1);"
             "}"
             "function unavailable(e) {"
             "  var text = String(e && e.name) + ' ' + String(e && e.message);"
             "  return text.indexOf('Server selection') >= 0 ||"
             "    text.indexOf('ECONNREFUSED') >= 0 ||"
             "    text.indexOf('MongoNetwork') >= 0 ||"
             "    text.indexOf('MongoServerSelection') >= 0;"
             "}"
             "async function handle(coll, mongodb, cmd) {"
             "  if (cmd.op === 'insert') {"
             "    try {"
             "      await coll.insertOne({"
             "        _id: cmd.id,"
             "        stream: cmd.stream,"
             "        seq: mongodb.Long.fromString(cmd.seq),"
             "        event: cmd.event"
             "      });"
             "      return {ok: true};"
             "    } catch (e) {"
             "      if (e && e.code === 11000) return {err: 'duplicate', message: String(e.message)};"
             "      throw e;"
             "    }"
             "  }"
             "  if (cmd.op === 'find-event') {"
             "    var byId = await coll.findOne({_id: cmd.id});"
             "    return {ok: byId ? byId.event : null};"
             "  }"
             "  if (cmd.op === 'find-slot') {"
             "    var bySlot = await coll.findOne({stream: cmd.stream, seq: mongodb.Long.fromString(cmd.seq)});"
             "    return {ok: bySlot ? bySlot.event : null};"
             "  }"
             "  if (cmd.op === 'find-range') {"
             "    var bounds = {$gte: mongodb.Long.fromString(cmd.from)};"
             "    if (cmd.to != null) bounds.$lte = mongodb.Long.fromString(cmd.to);"
             "    var ranged = await coll.find({stream: cmd.stream, seq: bounds}).sort({seq: 1}).toArray();"
             "    return {ok: ranged.map(function (doc) { return doc.event; })};"
             "  }"
             "  if (cmd.op === 'find-all') {"
             "    var all = await coll.find({}).toArray();"
             "    return {ok: all.map(function (doc) { return doc.event; })};"
             "  }"
             "  if (cmd.op === 'streams') {"
             "    return {ok: await coll.distinct('stream')};"
             "  }"
             "  if (cmd.op === 'drop') {"
             "    await coll.drop();"
             "    return {ok: true};"
             "  }"
             "  if (cmd.op === 'close') return {ok: true};"
             "  return {err: 'driver', message: 'unknown op'};"
             "}"
             "async function main() {"
             "  var mongodb;"
             "  try { mongodb = moduleApi.createRequire(workerData.requireFrom)('mongodb'); }"
             "  catch (e) { failBoot('driver-missing', String(e && e.message)); return; }"
             "  var client;"
             "  try {"
             "    client = new mongodb.MongoClient(workerData.uri);"
             "    await client.connect();"
             "    var coll = client.db(workerData.database).collection(workerData.collection);"
             "    await coll.createIndex({stream: 1, seq: 1}, {unique: true, name: 'clio_stream_seq'});"
             "    Atomics.store(header, 0, 0);"
             "    Atomics.notify(header, 0, 1);"
             "    for (;;) {"
             "      await waitFor(1);"
             "      var cmd = read();"
             "      var response;"
             "      try { response = await handle(coll, mongodb, cmd); }"
             "      catch (e) {"
             "        response = unavailable(e)"
             "          ? {err: 'unavailable', message: String(e && e.message)}"
             "          : {err: 'driver', message: String(e && e.message), code: e && e.code};"
             "      }"
             "      write(response);"
             "      Atomics.store(header, 0, 2);"
             "      Atomics.notify(header, 0, 1);"
             "      if (cmd.op === 'close') {"
             "        try { await client.close(); } catch (e) {}"
             "        return;"
             "      }"
             "    }"
             "  } catch (e) {"
             "    failBoot(unavailable(e) ? 'unavailable' : 'driver', String(e && e.message));"
             "    if (client) { try { await client.close(); } catch (ignore) {} }"
             "  }"
             "}"
             "main();"]))

(defn configured-uri
  []
  (let [uri (some-> js/process .-env .-CLIO_MONGO_URI)]
    (when-not (str/blank? uri) uri)))

(defn- with-timeout
  [uri]
  (if (re-find #"serverSelectionTimeoutMS=" uri)
    uri
    (str uri
         (if (str/includes? uri "?") "&" "?")
         "serverSelectionTimeoutMS=1500")))

(defn- encode-text [bytes text]
  (let [encoded (.encode (js/TextEncoder.) text)]
    (when (> (.-length encoded) (- (.-length bytes) 16))
      (throw (ex-info "MongoDB command exceeds the worker buffer"
                      {:clio/error :clio.mongo/too-large
                       :bytes (.-length encoded)})))
    (.set bytes encoded 16)
    (.-length encoded)))

(defn- decode-text [bytes n]
  (.decode (js/TextDecoder. "utf-8") (.subarray bytes 16 (+ 16 n))))

(defn- read-response [header bytes]
  (js->clj (.parse js/JSON (decode-text bytes (.load js/Atomics header 1)))
           :keywordize-keys true))

(defn- wait-state! [header bytes target timeout-ms]
  (let [deadline (+ (js/Date.now) timeout-ms)]
    (loop []
      (let [cur (.load js/Atomics header 0)]
        (cond
          (= cur target) target
          (= cur 9)
          (let [body (try (read-response header bytes) (catch :default _ {}))]
            (throw (ex-info (or (:message body) "MongoDB worker failed")
                            {:clio/error (case (:err body)
                                           "driver-missing" :clio.mongo/driver-missing
                                           :clio.mongo/unavailable)
                             :mongo/detail body})))
          :else
          (let [remaining (- deadline (js/Date.now))]
            (when-not (pos? remaining)
              (throw (ex-info "MongoDB worker timed out"
                              {:clio/error :clio.mongo/unavailable
                               :state cur})))
            (.wait js/Atomics header 0 cur remaining)
            (recur)))))))

(defn- command! [handle cmd]
  (let [{:keys [header bytes]} handle
        payload (.stringify js/JSON (clj->js cmd))
        len (encode-text bytes payload)]
    (when-not (= 0 (.load js/Atomics header 0))
      (throw (ex-info "MongoDB worker is not idle"
                      {:clio/error :clio.mongo/unavailable})))
    (.store js/Atomics header 1 len)
    (.store js/Atomics header 0 1)
    (.notify js/Atomics header 0 1)
    (wait-state! header bytes 2 20000)
    (let [response (read-response header bytes)]
      (.store js/Atomics header 0 0)
      (.notify js/Atomics header 0 1)
      response)))

(defn- raise! [response]
  (case (:err response)
    nil (:ok response)
    "duplicate" (throw (ex-info "MongoDB already has this id or stream slot"
                                {:clio/error :clio.mongo/duplicate
                                 :mongo/detail response}))
    "driver-missing" (throw (ex-info "MongoDB Node driver is not installed"
                                     {:clio/error :clio.mongo/driver-missing
                                      :mongo/detail response}))
    "unavailable" (throw (ex-info "MongoDB is unavailable"
                                  {:clio/error :clio.mongo/unavailable
                                   :mongo/detail response}))
    (throw (ex-info (or (:message response) "MongoDB driver error")
                    {:clio/error :clio.mongo/driver
                     :mongo/detail response}))))

(defn connect
  [{:keys [uri database collection]}]
  (let [buffer (js/SharedArrayBuffer. buffer-bytes)
        header (js/Int32Array. buffer 0 4)
        bytes (js/Uint8Array. buffer)
        _ (.store js/Atomics header 0 4)
        require-from (node-path/resolve (node-path/join (.cwd js/process) "package.json"))
        worker (worker-threads/Worker.
                worker-source
                #js {:eval true
                     :workerData #js {:buffer buffer
                                      :uri (with-timeout uri)
                                      :database database
                                      :collection collection
                                      :requireFrom require-from}})]
    (.on worker "error" (fn [_err] nil))
    (try
      (wait-state! header bytes 0 8000)
      {:worker worker :header header :bytes bytes :open? (atom true)}
      (catch :default cause
        (.terminate worker)
        (throw cause)))))

(defn close!
  [{:keys [worker header bytes open?]} drop?]
  (when (compare-and-set! open? true false)
    (try
      (when drop?
        (raise! (command! {:header header :bytes bytes} {:op "drop"})))
      (raise! (command! {:header header :bytes bytes} {:op "close"}))
      (catch :default _ nil)
      (finally
        (.terminate worker)))))

(defn- call [handle cmd]
  (when-not @(:open? handle)
    (throw (ex-info "MongoDB store is closed"
                    {:clio/error :clio.mongo/closed})))
  (raise! (command! handle cmd)))

(defn insert!
  [handle {:keys [id stream seq event]}]
  (call handle {:op "insert" :id id :stream stream :seq seq :event event})
  :inserted)

(defn find-event
  [handle id]
  (call handle {:op "find-event" :id id}))

(defn find-slot
  [handle stream seq]
  (call handle {:op "find-slot" :stream stream :seq seq}))

(defn find-range
  [handle stream from to]
  (vec (call handle {:op "find-range" :stream stream :from from :to to})))

(defn find-all
  [handle]
  (vec (call handle {:op "find-all"})))

(defn stream-names
  [handle]
  (vec (call handle {:op "streams"})))
