(ns testapp.jobs.basic
  (:require [onyx.job :refer [add-task register-job]]
            [onyx.tasks.core-async :as core-async-task]
            [onyx.plugin.kafka :as kafka]
            [onyx.tasks.kafka :as kafka-task]
            [testapp.shared :as shared]
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
        kafka-in-opts {:onyx/name :in
                       :onyx/plugin :onyx.plugin.kafka/read-messages
                       :onyx/type :input
                       :onyx/medium :kafka
                       :kafka/topic "my-message-stream"
                       :kafka/group-id "onyx-consumer"
                       :kafka/fetch-size 307200
                       :kafka/chan-capacity 1000
                       :kafka/zookeeper "127.0.0.1:2181"
                       :kafka/offset-reset :smallest
                       :kafka/force-reset? true
                       :kafka/empty-read-back-off 500
                       :kafka/commit-interval 500
                       :kafka/deserializer-fn :testapp.shared/deserialize-message-json
                       :kafka/wrap-with-metadata? false
                       :onyx/min-peers 1
                       :onyx/max-peers 1
                       :onyx/batch-size 100
                       :onyx/doc "Reads messages from a Kafka topic"}]
    (basic-job batch-settings kafka-in-opts)))
