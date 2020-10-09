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
            [meeseeks-db.utils :refer [fetch-object QueryExpression Key Value deref-of mangle-keys]])
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
      (car/del k))))

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

#_(defn default-id->iid
  ([conn id]
   (if-let [iid (wcar conn (car/get (str "id:" id)))]
     iid
     (let [iid (wcar conn (car/incr "next-iid"))]
       (when iid (car/atomic conn 10
                     (car/multi)
                     (car/setnx (str "id:" id) iid)
                     (car/setnx (str "iid:" iid) id)))
       (wcar conn (car/get (str "id:" id))))))
  ([conn id delete?]
   (wcar conn (car/get (str "id:" id)))))

;hard fix for nil users
(defn default-id->iid
  ([conn id]
   (if-let [iid (wcar conn (car/get (str "id:" id)))]
     (do (when (nil? (wcar conn (car/get (str "iid:" iid)))) (wcar conn (car/set (str "iid:" iid) id))) iid)
     (let [iid (:result (with-lock conn "kona-iid" 20000 50000 (wcar conn (car/incr "next-iid"))))]
       (when iid (wcar conn
                       (car/set (str "id:" id) iid)
                       (car/set (str "iid:" iid) id)
                       ))
       iid)))
  ([conn id delete?]
   (wcar conn (car/get (str "id:" id)))))


(defn default-multi-id->iid
  ([conn id_list]
     (let [save-id (fn [i-id-iid_list]
                       (when i-id-iid_list
                         (let [id_list (map :id i-id-iid_list)
                               iid_list (map :iid i-id-iid_list)
                               key_list (map #(str "iid:" %) iid_list)
                               result-id_list (wcar conn (apply car/mget key_list))
                               nil-key-id_list (filter #(nil? (first %)) (map list result-id_list key_list id_list))
                               key-id_list (flatten (map (fn [[nil-id key id]] [key id]) nil-key-id_list))]

                           (when (not-empty key-id_list) (wcar conn (apply car/mset key-id_list))))))

           iid_list (wcar conn (apply car/mget (doall (map #(str "id:" %) id_list))))
           i-id-iid_list (map-indexed (fn [i [id_i iid_i]] {:idx i :id id_i :iid iid_i}) (map list id_list iid_list))
           iid-found?i-id-iid_list (group-by (fn [idx-id-iid] (some? (:iid idx-id-iid))) i-id-iid_list)
           i-id-iid_list (get iid-found?i-id-iid_list true)

           i-id-nil_list (get iid-found?i-id-iid_list false)
           n-new-iids (count i-id-nil_list)
           last-new-iid (:result (with-lock conn "kona-iid" 20000 50000 (wcar conn (car/incrby "next-iid" n-new-iids))))

           i-id-new-iid_list (when (and (> n-new-iids 0) last-new-iid)
                                  (let [idx_list (map :idx i-id-nil_list)
                                        id_list (map :id i-id-nil_list)
                                        new-iid_list (range (+ 1 (- last-new-iid n-new-iids)) (+ 1 last-new-iid))

                                        id:new-iid_list (flatten (map list (map #(str "id:" %) id_list) new-iid_list))
                                        new-iid:id_list (flatten (map list (map #(str "iid:" %) new-iid_list) id_list))]

                                        (wcar conn (apply car/mset (concat id:new-iid_list new-iid:id_list)))

                                        (map (fn [[idx id new-iid]] {:idx idx :id id :iid new-iid})
                                             (map list idx_list id_list new-iid_list))))]

   (save-id i-id-iid_list)
   (map :iid (sort-by :idx (concat i-id-iid_list i-id-new-iid_list)))))

  ([conn id delete?]
   (wcar conn (doseq [id_i id] (car/get (str "id:" id_i))))))

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
   (s/optional-key :f-id->iid) (s/pred fn?)
   (s/optional-key :f-iid->id) (s/pred fn?)
   (s/optional-key :f-id->conn) (s/pred fn?)
   (s/optional-key :f-multi-id->iid) (s/pred fn?)})

(s/defn init [dbs :- (deref-of [Connection]) {:keys [f-index data-db f-id->iid f-iid->id f-id->conn
                                                     f-multi-id->iid ]
                                              :or   {f-id->iid  default-id->iid
                                                     f-iid->id  default-iid->id
                                                     f-id->conn default-id->conn
                                                     f-multi-id->iid default-multi-id->iid}} :- ClientConfig]
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
             :f-id->iid f-id->iid
             :f-id->conn f-id->conn
             :f-iid->id f-iid->id
             :f-index   f-index
             :f-multi-id->iid f-multi-id->iid}]
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

(defn group-by-idx [f coll]
  "group-by, except that f accept the idx of the element in coll"
  (persistent!
    (reduce (fn [ret idx]
              (let [k (f idx)
                    x (nth coll idx)]
                (assoc! ret k (conj (get ret k []) x))))
            (transient {}) (range (count coll)))))



(defn multi-index!
  "Index and store an object."
  [{:keys [db data-db f-multi-id->iid f-index f-id->conn]} obj_list]
  (let [db          (deref db)
        data-db     (deref data-db)
        conn-dconn->obj_list (let [id_list (map :id obj_list)
                                  conn_list (doall (map #(f-id->conn db %) id_list))
                                  data-conn_list (doall (map #(f-id->conn data-db %) id_list))]
                        (group-by-idx (fn [idx] [(nth conn_list idx) (nth data-conn_list idx)]) obj_list))
        todo (map (fn [[[conn data-conn] obj]]
                    (let [id (map :id obj)
                          iid (f-multi-id->iid conn id)
                          old (wcar data-conn (doall (map #(fetch-object %) id)))
                          old (flatten (conj [] old))

                          index-pairs (doall (map
                                               (fn [[old_i obj_i]]
                                                  (merge-with conj
                                                    (->> (indexify f-index old_i)
                                                         (map (fn [[k vs]] [k {:old vs}]))
                                                         (into {}))
                                                    (->> (indexify f-index obj_i)
                                                         (map (fn [[k vs]] [k {:new vs}]))
                                                       (into {}))))
                                               (map list old obj)))
                          ]
                      [conn data-conn obj iid index-pairs]))
                  (seq conn-dconn->obj_list))]

    (doseq [[conn data-conn obj_list iid_list index-pairs_list] todo]
      (let [iid-id_list (map list iid_list (map :id obj_list))
            result-id_list (wcar conn (apply car/mget (doall (map #(str "iid:" %) iid_list))))
            iid->id (doall (flatten (map first (filter (fn [[iid-id id?]] (nil? id?))
                                             (map list iid-id_list result-id_list)))))
            iid-index-pairs (filter
                              (fn [[iid_i index-pairs_i]] (and iid_i (not-empty index-pairs_i)))
                              (map list iid_list index-pairs_list))
            iid_list (doall (map first iid-index-pairs))]

            (when (seq iid->id) (wcar conn (apply car/mset iid->id)))

            (doseq [[iid_i index-pairs_i] iid-index-pairs]
               (wcar conn
                 (doseq [[k {:keys [old_i new_i]}] index-pairs_i]
                   (update-attr! iid_i k old_i new_i))))

            (when (seq iid_list) (wcar conn (apply car/sadd "total" iid_list)))

            (wcar data-conn
              (doseq [obj_i obj_list]
                (save-object! (:id obj_i) obj_i)))))))

(defn multi-unindex!
  "Remove the object and its indices. for each id in id-list"
  [{:keys [db data-db f-id->conn f-multi-id->iid f-index]} id_list]
  (let [db        @db
        data-db   @data-db
        conn-dconn->id_list (let [conn (doall (map #(f-id->conn db %) id_list))
                            data-conn (doall (map #(f-id->conn data-db %) id_list))]
                        (group-by-idx (fn [idx] [(nth conn idx) (nth data-conn idx)]) id_list))

        conn-dconn->id_list-iid_list-indices_list (doall (map
                  (fn [[[conn data-conn] id-list]]
                    (let [iid-list (flatten (conj [] (f-multi-id->iid conn id-list "delete")))

                          obj_list (flatten (conj [] (wcar data-conn (map #(fetch-object %) id-list))))
                          indices_list (map #(indexify f-index %) obj_list)]

                      [conn data-conn id-list iid-list indices_list]))
                  (seq conn-dconn->id_list)))]

    (doseq [[conn data-conn id_list iid-list indices_list] conn-dconn->id_list-iid_list-indices_list]
      (wcar conn
            (doseq [[iid_i indices_i] (map list iid-list indices_list)]
              (doseq [[k vs] indices_i]
                (update-attr! iid_i k vs nil)))
            (apply car/srem "total" iid-list))

      (wcar data-conn
        (doseq [id_i id_list]
          (save-object! id_i nil))))))

(defn fetch
  "Fetch object by ID"
  [{:keys [data-db f-id->conn]} id & [fields]]
  (let [data-db (deref data-db)
        conn    (f-id->conn data-db id)]
    (wcar conn (fetch-object id fields))))

(defn delete-custom-attribute [{:keys [db]} attribute-name]
  (let [db (deref db)]
    (doall (pmap #(wcar % (car/del (str "custom:" (name attribute-name)))) db))))

(defn create-custom-attribute [{:keys [db] :as client} attribute-name ids]
  (let [db (deref db)
        id->conn (:f-id->conn client)
        db-buckets (group-by #(id->conn db %) ids)]
    (doall (pmap (fn [[db ids]]
            (when (not-empty ids)
              (let [iids (wcar db (apply car/mget (map #(str "id:" %) ids)))
                    key-name (str "custom:" (name attribute-name))]
                (wcar db
                      (apply car/sadd key-name iids)
                      (car/expire key-name 3600))))) db-buckets))))

(defn fix-custom-keys* [query mangle-map]
  (if (empty? (:nested query))
    (assoc query :name (get mangle-map (:name query) (:name query)))
    (update query :nested (fn [x] (map #(fix-custom-keys* % mangle-map) x)))))

(defn fix-custom-keys [query mangle-map]
  (let [mangle-map (into {} (map #(hash-map (str "custom:" (name (first %))) (str "custom:" (name (second %)))) mangle-map))]
    (fix-custom-keys* query mangle-map)))

(s/defn query :- {:size s/Int :sample [{Key s/Any}]}
  [client &
   [query :- (s/maybe QueryExpression)
    sample-size :- (s/maybe s/Int)
    fields :- [Key]
    custom-attributes :- {s/Keyword [s/Str]}]]
  (let [mangled-custom-attributes (mangle-keys custom-attributes)
        compiled-query (q/compile-query query)
        compiled-query (if (not-empty mangled-custom-attributes)
                         (fix-custom-keys compiled-query (zipmap (keys custom-attributes) (keys mangled-custom-attributes)))
                         compiled-query)]
    (try
      (doseq [[name ids] mangled-custom-attributes] (create-custom-attribute client name ids))
      (with-open [cursor (c/create-cursor! client compiled-query)]
        {:size   (c/cursor-size cursor)
         :sample (if (and (int? sample-size) (pos? sample-size))
                   (c/sample-cursor cursor sample-size fields)
                   [])})
      (finally
        (doseq [custom-attribute-key (keys mangled-custom-attributes)] (delete-custom-attribute client custom-attribute-key))))))

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
                                      (apply car/del ks))))))
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
