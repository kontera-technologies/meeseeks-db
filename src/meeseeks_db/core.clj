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

(ns meeseeks-db.core
  (:require [taoensso.carmine :as car :refer [wcar]]
            [taoensso.carmine.locks :refer [with-lock]]
            [taoensso.nippy :refer [freeze lzma2-compressor]]
            [clojure.set :refer [difference]]
            [clojure.string :refer [starts-with? replace-first]]
            [meeseeks-db.query :as q]
            [meeseeks-db.cursor :as c]
            [schema.core :as s]
            [meeseeks-db.utils :refer [fetch-object QueryExpression Key Value deref-of]])
  (:import [clojure.lang Murmur3]))

;; ===========================================================================
;; utils

(defn attr [key value]
  (meeseeks-db.utils/attr key value))
(defn- freeze-hashmap [m]
  (->> m
       (map (fn [[k v]]
              [k (if (coll? v)
                   (freeze v {:compressor lzma2-compressor})
                   v)]))
       (into {})))

(defn- save-object! [id obj]
  (let [k (str "p:" id)]
    (if obj
      (car/hmset* k (freeze-hashmap obj))
      (car/unlink k))))

(defn- update-attr! [id prefix old new]
  (let [s1     (set old)
        s2     (set new)
        to-del (difference s1 s2)
        to-add (difference s2 s1)
        k      (partial attr prefix)]
    (doseq [v to-del] (car/srem (k v) id))
    (doseq [v to-add] (car/sadd (k v) id))))

(defn- walk-keys [conn cursor pattern]
  (let [[cursor ks] (wcar conn
                          (car/scan cursor
                                    :match pattern
                                    :count 1000))]
    [(Long/parseLong cursor) ks]))

(defn- scan-indices* [conn pattern f]
  (loop [[cursor ks] (walk-keys conn 0 pattern)]
    (f ks)
    (when-not (zero? cursor)
      (recur (walk-keys conn cursor pattern)))))

(defn- indexify [f-index obj]
  (letfn [(normalize [vs]
            (remove nil? (if (coll? vs) vs [vs])))]
    (map (fn [[k vs]]
           [k (normalize vs)])
         (if (empty? obj) [] (f-index obj)))))

;; ===========================================================================
;; API

;; default id<->iid mappers

(defn default-id->iid
  ([conn id]
   (if-let [iid (wcar conn (car/get (str "id:" id)))]
     iid
     (let [iid (wcar conn (car/incr "next-iid"))
           added? (= 1
                     (wcar conn
                           (car/msetnx
                             (str "id:" id) iid
                             (str "iid:" iid) id)))]
       (if added?
         iid
         (if-let [final-iid (wcar conn (car/get (str "id:" id)))]
           final-iid
           (throw (ex-info (str "Failed to set iid" id iid) {:id id :attempted-iid iid})))))))
  ([conn id delete?]
   (wcar conn (car/get (str "id:" id)))))

(defn default-iid->id [iid]
  (car/get (str "iid:" iid)))

(defn default-id->conn [db id]
  (if (> (count db) 1)
    (let [hc (Murmur3/hashUnencodedChars (str id))]
      (nth db (mod hc (count db))))
    (first db)))

;; API
(s/defschema Connection
  "Carmine connection spec"
  {(s/optional-key :pool) s/Any
   :spec (s/pred map?)})
(s/defschema ClientConfig
  {:f-index (s/pred fn?)
   (s/optional-key :data-db) (deref-of [Connection])
   (s/optional-key :ttl) (s/maybe s/Int)
   (s/optional-key :f-id->iid) (s/pred fn?)
   (s/optional-key :f-iid->id) (s/pred fn?)
   (s/optional-key :f-id->conn) (s/pred fn?)})

(s/defn init [dbs :- (deref-of [Connection]) {:keys [f-index data-db ttl f-id->iid f-iid->id f-id->conn]
                                              :or   {f-id->iid  default-id->iid
                                                     f-iid->id  default-iid->id
                                                     f-id->conn default-id->conn}} :- ClientConfig]
  "Initialize meeseeks client

  Options:
  :f-index    - Function from domain object maps to indices (required)
  :data-db    - Optionally use different Redis connections for storing domain object maps
  :f-id->iid  - Function from connection and ID to IID for that connection
  :f-iid->id  - Function from IID to the original ID. Run in the context of `wcar`
  :f-id->conn - Function from list of DBs (either dbs or data-db) and ID to the appropriate connection


  "
  (assert (ifn? f-index)  "f-index function is mandatory")
  (let [mdb {:db        dbs
             :data-db   (or data-db dbs)
             :ttl ttl
             :f-id->iid f-id->iid
             :f-id->conn f-id->conn
             :f-iid->id f-iid->id
             :f-index   f-index}]
    mdb))

(defn index!
  "Index and store an object."
  [{:keys [db data-db f-id->iid f-index f-id->conn]} {:keys [id] :as obj}]
  (let [db          (deref db)
        data-db     (deref data-db)
        conn        (f-id->conn db id)
        iid         (f-id->iid conn id)
        data-conn   (f-id->conn data-db id)
        old         (wcar data-conn (fetch-object id))
        index-pairs (merge-with conj
                                (->> (indexify f-index old)
                                     (map (fn [[k vs]] [k {:old vs}]))
                                     (into {}))
                                (->> (indexify f-index obj)
                                     (map (fn [[k vs]] [k {:new vs}]))
                                     (into {})))]

    (when (and iid (not-empty index-pairs))
      (wcar conn
            (do
              (when (empty? (car/get (str "iid:" iid)))
                (car/set (str "iid:" iid) id))
              (doseq [[k {:keys [old new]}] index-pairs]
                (update-attr! iid k old new)))
            (car/sadd "total" iid)))

    (wcar data-conn
          (save-object! id obj))
    [id iid data-conn]))

(defn unindex!
  "Remove the object and its indices."
  [{:keys [db data-db f-id->iid f-index f-id->conn]} id]
  (let [db        @db
        data-db   @data-db
        conn      (f-id->conn db id)
        data-conn (f-id->conn data-db id)
        iid       (f-id->iid conn id "delete")
        obj       (wcar data-conn (fetch-object id))
        indices   (indexify f-index obj)]
    (wcar conn
          (doseq [[k vs] indices]
            (update-attr! iid k vs nil))
          (car/srem "total" iid))

    (wcar data-conn
          (save-object! id nil))))


(defn fetch
  "Fetch object by ID"
  [{:keys [data-db f-id->conn]} id & [fields]]
  (let [data-db (deref data-db)
        conn    (f-id->conn data-db id)]
    (wcar conn (fetch-object id fields))))

(s/defn query :- {:size s/Int :sample [{Key s/Any}]}
  [client &
   [query :- (s/maybe QueryExpression)
    sample-size :- (s/maybe s/Int)
    fields :- [Key]]]
  (with-open [cursor (c/create-cursor! client (q/compile-query query))]
    {:size   (c/cursor-size cursor)
     :sample (if (pos-int? sample-size)
               (c/sample-cursor cursor sample-size fields)
               [])}))

(defn scan-indices [{:keys [db]} pattern f]
  (dorun (pmap #(scan-indices* % pattern f) db)))

(defn remove-all! [{:keys [db data-db]}]
  (let [db (deref db)
        data-db (deref data-db)]
    (dorun
     (pmap #(wcar % (car/flushdb)) db))
    (dorun
     (pmap (fn [conn]
             (scan-indices* conn "p:*"
                            (fn [ks]
                              (when (seq ks)
                                (wcar conn
                                      (apply car/unlink ks))))))
           data-db))))

(defn memory-status [{:keys [db data-db]}]
  (letfn [(status [conn]
            (let [[[_ max-memory] {:keys [used-memory]}]
                  (wcar conn
                        (car/config-get :maxmemory)
                        (car/info* true))
                  max-memory  (Long/parseLong max-memory)
                  used-memory (Long/parseLong used-memory)]
              {:capacity max-memory
               :used     used-memory}))
          (aggregate [statuses]
                     {:raw      statuses
                      :capacity (reduce + (map :capacity statuses))
                      :used     (reduce + (map :used     statuses))})]
    {:db      (aggregate (pmap status db))
     :data-db (aggregate (pmap status data-db))}))
