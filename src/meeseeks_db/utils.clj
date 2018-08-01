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
            [clojure.core.async :as async :refer [<! <!! >! >!! go-loop]]
            [clojure.stacktrace :as st]))


(defprotocol Queryable
  (->query-expression [this]))

(s/defschema Named (s/cond-pre s/Str s/Keyword s/Symbol))
(s/defschema Op (s/enum :and :or :not 'and 'or 'not "and" "or" "not"))
(s/defschema Key Named)
(s/defschema Value (s/either Named s/Num))
(s/defschema Attr (s/constrained Named #(re-find #"^[^:]+:[^:]+$|total" (name %)) 'Attr))

(s/defschema QueryMap
  {(s/either Key [Key] #{Key}) (s/either Value [Value] #{Value})})

(s/defschema QueryExpression
  (s/cond-pre
    [(s/one Op "op")
     (s/recursive #'QueryExpression)]
    (s/protocol Queryable)
    Attr))


(def ^:dynamic *max-workers* 20)

(def ^:dynamic *exception-handler*
  (fn [ex]
    (locking *out*
      (st/print-stack-trace ex))
    nil))

(defn run-command
  ([conns m r rinit]
   (run-command conns m r rinit *exception-handler*))
  ([conns m r rinit ex-handler]
   (if (> (count conns) 2)
     (let [n (min (count conns) *max-workers*)
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
     (reduce r rinit (map m conns)))))

(defn stringify [o]
  (cond
    (string? o)  o
    (keyword? o) (name o)
    :else        (str o)))

(defn attr [key value]
  (str (stringify key) \: (stringify value)))

(defn translate-iids [conn iid->id iids]
  (let [ids (wcar conn :as-pipeline (doseq [iid iids]
                                      (iid->id iid)))]
    ids))


(defn fetch-object [id & [fields]]
  (let [k (str "p:" id)]
    (if fields
      (apply car/hmget* k fields)
      (car/parse-map (car/hgetall k) :keywordize))))