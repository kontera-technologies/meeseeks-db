;  Copyright 2018 Amobee Inc.
;     This file is part of meeseeks-db.
;
;    meeseeks-db is free software: you can redistribute it and/or modify
;    it under the terms of the GNU General Public License as published by
;    the Free Software Foundation, either version 3 of the License, or
;    (at your option) any later version.
;
;    meeseeks-db is distributed in the hope that it will be useful,
;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;    GNU General Public License for more details.
;
;    You should have received a copy of the GNU General Public License
;    along with meeseeks-db.  If not, see <https://www.gnu.org/licenses/>.

(ns meeseeks-db.utils
  (:require [taoensso.carmine :as car :refer [wcar]]
            [schema.core :as s]
            [clojure.core.async     :as async :refer [<! <!! >! >!! go-loop]])
  (:import (clojure.lang Murmur3)))

(s/defschema Named (s/cond-pre s/Str s/Keyword s/Symbol))
(s/defschema Op (s/enum :and :or :not 'and 'or 'not "and" "or" "not"))
(s/defschema Key Named)
(s/defschema Value (s/either Named s/Num))
(s/defschema Attr (s/constrained Named #(re-find #"^[^:]+:[^:]+$|total" (name %)) 'Attr))

(s/defschema QueryMap
  {(s/either Key [Key]) (s/either Value [Value] #{Value})})

(s/defschema QueryExpression
  (s/cond-pre
    [(s/one Op "op")
     (s/recursive #'QueryExpression)]
    QueryMap
    Attr))

(s/defschema Query {:name Named
                    (s/optional-key :op) (s/enum :and :or :not)
                    :transient? s/Bool
                    (s/optional-key :nested) [(s/recursive #'Query)]})

(def max-workers 20)

(defn run-command [conns m r rinit ex-handler]
  (if (> (count conns) 2)
    (let [n (min (count conns) max-workers)
          in-ch (async/chan)
          out-chs (doall (for [_ (range n)] (async/chan)))]
      (doseq [out-ch out-chs]
           (async/thread
             (loop []
               (when-some [conn (<!! in-ch)]
                 (let [res (try
                             (m conn)
                             (catch Exception ex
                               (ex-handler ex)))]
                   (>!! out-ch res)
                   (recur))))
             (async/close! out-ch)))
      (async/onto-chan in-ch conns)
      (<!! (async/reduce r rinit (async/merge out-chs))))
    (reduce r rinit (map m conns))))

(defn id->conn [db id]
  (if (> (count db) 1)
    (let [hc (Murmur3/hashUnencodedChars (str id))]
      (nth db (mod hc (count db))))
    (first db)))

(defn stringify [o]
  (cond
    (string? o)  o
    (keyword? o) (name o)
    :else        (str o)))

(defn attr [key value]
  (str (stringify key) \: (stringify value)))

(defn translate-iids [conn iid->id iids]
  (let [ids (wcar conn (doall (map iid->id iids)))]
    (if (or (nil? ids) (coll? ids))
      ids
      (list ids))))


(defn fetch-object [id & [fields]]
  (let [k (str "p:" id)]
    (if fields
      (apply car/hmget* k fields)
      (car/parse-map (car/hgetall k) :keywordize))))


(defn fetch-objects [db ids fields]
  (let [conn->ids (reduce (fn [acc id]
                            (let [conn (id->conn db id)]
                              (update-in acc [conn] conj id)))
                          {}
                          ids)]
    (doall
      (mapcat (fn [[conn ids]]
                (if (= (count ids) 1)
                  [(into {} (wcar conn (fetch-object (first ids) fields)))]
                  (wcar conn (doall (map #(fetch-object % fields) ids)))))
              conn->ids))))