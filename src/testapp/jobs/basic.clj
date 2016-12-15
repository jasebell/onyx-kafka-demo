(ns testapp.jobs.basic
  (:require [onyx.job :refer [add-task register-job]]
            [onyx.tasks.core-async :as core-async-task]
            [onyx.plugin.kafka :as kafka]
            [onyx.tasks.kafka :as kafka-task]
            [testapp.shared :as shared]
            [onyx.plugin.s3-output :as s3out]
            [onyx.plugin.s3-utils :as s3util]
            [testapp.tasks.math :as math]))

(defn basic-job
  [batch-settings kafka-opts]
  (let [base-job {:workflow [[:in :inc]
                             [:inc :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (kafka-task/consumer :in kafka-opts))
        (add-task (math/process-kafka :inc batch-settings))
        (add-task (core-async-task/output :out batch-settings)))))

(defmethod register-job "kafka-job"
  [job-name config]
  (let [batch-settings {:onyx/batch-size 1
                        :onyx/batch-timeout 1000
                        :onyx/min-peers 1
                        :onyx/max-peers 1}
        s3-opts  {:onyx/name :out
                  :onyx/plugin :onyx.plugin.s3-output/output
                  :s3/bucket "onyx-output"
                  :s3/encryption :none
                  :s3/serializer-fn :clojure.core/pr-str
                  :s3/key-naming-fn :onyx.plugin.s3-output/default-naming-fn
                  :onyx/type :output
                  :onyx/medium :s3
                  :onyx/batch-size 20
                  :onyx/doc "Writes segments to s3 files, one file per batch"}
        kafka-in-opts  {:onyx/name :in
                        :onyx/plugin :onyx.plugin.kafka/read-messages
                        :onyx/type :input
                        :onyx/medium :kafka
                        :kafka/topic "my-message-stream"
                        :kafka/group-id "onyx-consumer"
                        :kafka/fetch-size 307200
                        :kafka/chan-capacity 1000
                        :kafka/zookeeper "192.168.1.71:2181"
                        :kafka/offset-reset :smallest
                        :kafka/force-reset? true
                        :kafka/empty-read-back-off 500
                        :kafka/commit-interval 500
                        :kafka/deserializer-fn :testapp.shared/deserialize-message-json
                        :kafka/wrap-with-metadata? false
                        :onyx/min-peers 1
                        :onyx/max-peers 1
                        :onyx/batch-size 100
                        :onyx/doc "Reads messages from a Kafka topic"}
        aws-client (s3util/set-region (s3util/new-client) "eu-west-1")]
    (basic-job batch-settings kafka-in-opts)))
