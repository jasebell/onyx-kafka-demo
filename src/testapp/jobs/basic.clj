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
  (let [batch-settings (:batch-config config)
        kafka-in-opts (:kafka-config config)]
    (basic-job batch-settings kafka-in-opts)))
