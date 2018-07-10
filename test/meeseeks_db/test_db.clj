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

(ns meeseeks-db.test-db
  (:require [meeseeks-db.core      :as c]
            [taoensso.carmine      :as car :refer [wcar]]
            [clojure.core.reducers :as r]))

(def redis-ports [26269 26279 26289])
(def ^:dynamic *docker-host* nil)

(defn cleanup-db! [{:keys [db]}]
  (let [db (deref db)]
    (doseq [conn db]
     (wcar conn (car/flushall)))))

(defn initialize-client
  "Initializes meeseeks-db client.
  Assumes there are two redis instances running in docker.
  Will attempt to find out how docker is running (natively or via boot2docker).
  You can overwrite the docker host by binding *docker-host* variable to another value.
  Will clean the database after opening connections!"
  [f-index]
  (let [docker-host (or *docker-host* (System/getenv "DOCKER_HOST"))
        redis-host  (if docker-host
                      (last (re-find #"tcp://([^:]+):.*$" docker-host))
                      "localhost")
        redis-uris  (map #(str "redis://" redis-host ":" %) redis-ports)
        redis-configs (for [uri redis-uris] {:pool {} :spec {:uri uri :timeout-ms 8000}})
        client      (c/init (atom redis-configs)
                            {:f-index f-index})]
    (cleanup-db! client)
    client))

(defn index-entities! [client entities]
  (r/foldcat (r/map (partial c/index! client) entities)))
