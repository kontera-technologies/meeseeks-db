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
            [meeseeks-db.utils :refer [translate-iids run-command fetch-objects
                                       ;; Schemas
                                       Attr Op Key Query]]
            [clojure.core.reducers :as r]
            [clojure.stacktrace :as st]
            [schema.core :as s]
            [taoensso.carmine :as car :refer [wcar]])
  (:import [java.lang AutoCloseable]))

(defn- create-cursor!* [client query]
  (q/query->cursor client query)
  query)

(defn- cursor-seq* [conns iid->id name]
  (when-not (empty? conns)
    (let [conn (first conns)]
      (lazy-cat (translate-iids conn iid->id (wcar conn (car/smembers name)))
                (cursor-seq* (rest conns) iid->id name)))))


(defn- cleanup-query-cursor* [query conn]
  (when (:transient? query)
    (wcar conn (car/del (:name query)))))

(defn- cleanup-query-cursor [db query]
  (r/reduce conj [] (r/map (partial cleanup-query-cursor* query) @db)))

(defrecord Cursor [name
                     transient?
                     op 
                     nested
                     db
                     data-db
                     iid->id]
  AutoCloseable
  (close [this]
    (cleanup-query-cursor db this)))

(s/defn create-cursor! :- Cursor
  [{:keys [db data-db f-iid->id] :as client} query :- Query]
  (map->Cursor (assoc (create-cursor!* client query)
                 :db db
                 :data-db data-db
                 :iid->id f-iid->id)))

(s/defn cursor-seq [cursor]
  (let [{:keys [name db iid->id]} cursor]
    (cursor-seq* @db iid->id name)))

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


(defn cursor-size [cursor]
  "Creates a cursor and returns the size of the resulting query"
  (run-command @(:db cursor)
               (fn [conn]
                 (wcar conn (car/scard (:name cursor))))
               (fnil + 0 0) 0
               (fn [ex]
                 (locking *out*
                   (st/print-stack-trace ex))
                 0)))


(s/defn sample-cursor :- [{s/Keyword s/Any}]
  [cursor :- Cursor
    sample-size :- (s/maybe s/Int) & 
    [fields :- [Key]]]
  (if (pos? (or sample-size 0))
    (let [conns        @(:db cursor)
          data-db      @(:data-db cursor)
          iid->id      (:iid->id cursor)
          cursor-name    (:name cursor)
          sample-size* (+ (long (Math/ceil (/ sample-size (count conns)))) 100)]
      (->> (run-command conns
                        (partial sample-cursor*
                                 cursor-name
                                 sample-size*
                                 iid->id
                                 data-db
                                 fields)
                        into []
                        (fn [ex]
                          (locking *out*
                            (st/print-stack-trace ex))
                          []))
           shuffle
           (take sample-size)))
    '()))

(defn destroy-cursor! [cursor]
  (.close cursor))

