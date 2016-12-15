(defproject testapp "0.1.0-SNAPSHOT"
  :description ""
  :url ""
  :license {:name ""
            :url ""}
  :dependencies [[aero "1.0.0-beta2"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [cheshire "5.5.0"]
                 [org.onyxplatform/onyx "0.9.15"]
                 [org.onyxplatform/lib-onyx "0.9.15.0"]
                 [org.onyxplatform/onyx-amazon-s3 "0.9.15.0"]
                 [org.onyxplatform/onyx-kafka-0.8 "0.9.15.0"]]
  :source-paths ["src"]

  :profiles {:dev {:jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
                   :global-vars {*assert* true}}
             :dependencies [[org.clojure/tools.namespace "0.2.11"]
                            [lein-project-version "0.1.0"]]
             :uberjar {:aot [lib-onyx.media-driver
                             testapp.core]
                       :uberjar-name "peer.jar"
                       :global-vars {*assert* false}}})
