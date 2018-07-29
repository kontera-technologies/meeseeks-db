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

(ns meeseeks-db.cursor
  (:require [meeseeks-db.query :as q]
            [meeseeks-db.utils :refer [translate-iids run-command fetch-objects Queryable
                                       ;; Schemas
                                       Attr Op Key QueryExpression]]
            [schema.core :as s]
            [taoensso.carmine :as car :refer [wcar]])
  (:import [java.lang AutoCloseable]
           [meeseeks_db.query Query]))

(s/defrecord Cursor [query :- Query
                     size :- s/Int
                     client]
  AutoCloseable
  (close [_this]
    (q/cleanup-query client query))
  Queryable
  (->query-expression [_this]
    (:name query)))


(s/defn create-cursor! :- Cursor
  [client
   query :- (s/cond-pre Query QueryExpression)]
  (let [query (if (instance? Query query)
                query
                (q/compile-query query))
        size (q/run-query! client query)]
    (->Cursor query size client)))

(defn- cursor-seq* [conns iid->id name]
  (mapcat (fn [conn] (translate-iids conn iid->id (wcar conn (car/smembers name)))) conns)
  (when-not (empty? conns)
    (let [conn (first conns)]
      (lazy-cat (translate-iids conn iid->id (wcar conn (car/smembers name)))
                (cursor-seq* (rest conns) iid->id name)))))

(s/defn cursor-seq [cursor]
  (let [{:keys [client query]} cursor
        {:keys [db f-iid->id]} client]
    (cursor-seq* @db f-iid->id (:name query))))

(s/defn cursor-size :- s/Int
  [cursor :- Cursor]
  "Returns the size of the query"
  (:size cursor))


(defn- sample-cursor* [view-name sample-size iid->id data-db fields conn]
  (let [sample (->> (wcar conn
                          (car/srandmember view-name sample-size))
                    (translate-iids conn iid->id))]
    (if (or (empty? fields)
            (and (= 1 (count fields)) (= :id (first fields))))
      (map #(hash-map :id %) sample)
      (if (seq sample)
        (fetch-objects data-db sample fields)
        '()))))


(s/defn sample-cursor :- [{Key s/Any}]
  [cursor :- Cursor
    sample-size :- (s/maybe s/Int) & 
    [fields :- [Key]]]
  (if (pos? (or sample-size 0))
    (let [{:keys [client query]} cursor
          conns        @(:db client)
          data-db      @(:data-db client)
          iid->id      (:f-iid->id client)
          cursor-name    (:name query)
          sample-size* (+ (long (Math/ceil (/ sample-size (count conns)))) 100)]
      (->> (run-command conns
                        (partial sample-cursor*
                                 cursor-name
                                 sample-size*
                                 iid->id
                                 data-db
                                 fields)
                        into [])
           shuffle
           (take sample-size)))
    '()))

(defn destroy-cursor! [cursor]
  (.close cursor))

