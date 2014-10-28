(ns pluqi-simplified-data.pipeline
  (:require [grafter.tabular :refer [melt column-names columns rows all-columns
                                     derive-column mapc swap drop-rows open-all-datasets
                                     make-dataset move-first-row-to-header _]]
            [incanter.core :refer [to-list]]
            [grafter.rdf :refer [graph-fn graph s]]
            [pluqi-simplified-data.prefix :refer :all]
            [pluqi-simplified-data.transform :refer [->integer]]))

(defn fix-header [dataset]
  (let [[year-row region-row & rest-rows] (to-list dataset)

        merged-row (map #(str %1 "-" %2) (drop 1 year-row) (drop 1 region-row))]

    (make-dataset dataset
                  (concat [:type-of-crime] merged-row rest-rows))))

(defn pipeline [dataset]
  (-> dataset
      fix-header
      (drop-rows 2)
      (melt :type-of-crime)))
