(ns testapp.tasks.math
  (:require [schema.core :as s]))

(defn get-data [fn-data]
  (println fn-data))

(s/defn process-kafka
  ([task-name :- s/Keyword task-opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/type :function
                             :onyx/fn ::get-data}
                            task-opts)}}))
