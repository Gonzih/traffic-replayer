(ns traffic-replayer.core
  (:require [clojure.java.io :refer [reader writer]]
            [clojure.string :as string]
            [aleph.http :as http]
            [aleph.flow :as flow]
            [manifold.deferred :as d])
  (:import [java.io BufferedReader PrintWriter]
           [java.text SimpleDateFormat])
  (:gen-class))

(def stats-callback prn)

(defonce static-client-connection-pool
  (http/connection-pool
    {:response-executor
     (flow/utilization-executor
       0.9 2500
       {:stats-callback (partial stats-callback :client)})
     :connections-per-host 5000
     :total-connections 10000
     :target-utilization 0.9
     :stats-callback (partial stats-callback :connection)
     :connection-options {:keep-alive? true}}))

(defrecord entry [date request])
(defrecord request [url start-time])

(def logfile (atom nil))

(def logger (agent 0))

(defn log-response [url {:keys [request-time status]} i]
  (let [msg (format "%d\t%s\t%d\t%dms\n" i url status request-time)]
    (spit @logfile msg))
  (inc i))

(defn hit-url!
  "Request a URL and register the socket to receive a response."
  [url]
  (try
    (d/catch Exception #(println "Exception in aleph manifold execution: " %)
      (d/chain
        (http/get
          url
          {:pool static-client-connection-pool
           :pool-timeout 10000
           :throw-exceptions false
           :connection-timeout 10000
           :request-timeout 60000})
        (fn [response]
          (send logger (partial log-response url response)))))
    (catch Exception e
      (printf "Exception during hit-url call: %s\n" e))))

(defn write-script
  "Generate a script file from a HAProxy input stream."
  [^BufferedReader in ^PrintWriter out]
  (let [interesting-line-re #" \[([^ ]*)\] .*\"GET (.*) HTTP"
        date-parser (SimpleDateFormat. "dd/MMM/yyyy:hh:mm:ss.SSS")]
    (doseq [{:keys [date request]}
            (for [^String line (line-seq in)
                  :let [entry (let [[date request]
                                    (rest (re-find interesting-line-re line))]
                                (try
                                  (entry. (.. date-parser (parse date) getTime)
                                          request)
                                  (catch Exception _ nil)))]
                  :when entry]
              entry)]
      (.println out (str date "\t" request)))))


(defn replay-script
  "Replay a script to a specified url base, logging interesting stuff as we go."
  [^BufferedReader in ^String urlbase ^String log]
  ; (future (response-collector log))
  (reset! logfile log)
  (let [lines (line-seq in)]
    (doseq [[[time1 _] [time2 request]]
            (partition 2 1
                       (map (fn [^String line]
                              (let [[time request] (.split line "\t" 2)]
                                [(Long/parseLong time) request]))
                            (cons (first lines) lines)))]
      (let [delay (- time2 time1)]
        (if (> delay 5)
          (do (println "Sleeping" delay "ms before next request")
              (Thread/sleep delay))
          (println "No sleep!  Kaboom!")))
      (hit-url! (str urlbase request)))))

(defn -main [& args]
  (let [cmd (first args)
        arguments (rest args)]
    (cond
      (= cmd "generate")
      (let [[infile outfile] arguments
            sorter (.exec (Runtime/getRuntime)
                          (into-array ["sort" "-n" "-k1" "-o" outfile]))]
        (with-open [in (reader infile)
                    out (writer (.getOutputStream sorter))]
          (write-script in out)))

      (= cmd "replay")
      (let [[script urlbase logfile] arguments]
        (with-open [in (reader script)]
          (replay-script in (string/replace urlbase #"/+$" "") logfile)))

      :else
      (do (println
            "Usage: generate <haproxy log file> <output script>; or\n"
            "      replay   <script file> http://some/base/url <log file>"))))
  (shutdown-agents))
