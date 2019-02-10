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
  (:require [meeseeks-db.core :as c]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.core.reducers :as r]
            [meeseeks-db.utils :as u]
            [clojure.core.logic :as l]
            [clojure.core.logic.fd :as lf]
            [clojure.set :as set]
            [clojure.walk :refer [prewalk postwalk]]
            [clojure.string :as string]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck.generators :as gen']
            [clojure.test.check.random :as random]
            [clojure.test.check.rose-tree :as rose]
            [midje.sweet :as m]))

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


; For deterministic testing
(defn sample [gen res-count & [seed]]
  (let [rngs (gen/lazy-random-states (random/make-random (or seed 7)))
        sizes (gen/make-size-range-seq 200)]
    (take res-count (map #(rose/root (gen/call-gen gen %1 %2)) rngs sizes))))

#_(def sample gen/sample)

(defn prop-is? [k v o]
  (= v (get o k)))

(defn prop-has? [k v o]
  (boolean ((set (get o k)) v)))

(defn prop-visited? [domain o]
  (prop-has? :td domain o))

(m/defchecker attr [name]
              (m/every-checker
                (m/contains {:name name :transient? false})))

(m/defchecker node [op & children]
              (m/contains {:name       (m/has-prefix "tmp:")
                           :op         op
                           :transient? true
                           :nested     (m/just children :in-any-order)}))
;; generators

(def property-gen
  (gen/hash-map
    :id (gen/fmap (fn [id] (str "P" id)) gen/pos-int)
    :datatype (gen/elements [:string :quantity :globe-coordinate :wikimedia-item :time])
    :label (gen/not-empty gen/string-alphanumeric)
    :description gen/string-alphanumeric))

(def profile-gen-map {:id                gen/uuid
                      :gender            (gen/elements [:male :female])
                      :age               (gen/elements [:2-11 :12-17 :18-24 :25-34 :35-44 :45-54 :55-64 :65+])
                      :income            (gen/elements [:0-15k :15-25k :25-40k :40-60k :60-75k :75-100k :100k+])
                      :race              (gen/elements [:black :white])
                      :cc                (gen/elements ["us" "gb" "sg"])
                      :frequent-keywords (gen/list-distinct
                                           (gen/frequency [[1 (gen/elements [1 2 3 4 5 6 7 8 9 10])]
                                                           [2 (gen/large-integer* {:min 10 :max 40000000})]]))
                      :uw                (gen/list-distinct
                                           (gen/frequency [[1 (gen/elements [1 2 3 4 5 6 7 8 9 10])]
                                                           [2 (gen/large-integer* {:min 10 :max 40000000})]]))
                      :gsw               (gen/list-distinct
                                           (gen/frequency [[1 (gen/elements [1 2 3 4 5 6 7 8 9 10])]
                                                           [2 (gen/large-integer* {:min 10 :max 40000000})]]))
                      :td                (gen/list-distinct
                                           (gen/frequency [[1 (gen/elements ["google.com"
                                                                             "facebook.com"
                                                                             "yahoo.com"
                                                                             "youtube.com"
                                                                             "bing.com"
                                                                             "duckduckgo.com"])]
                                                           [2 (gen/fmap (fn [[name suffix]]
                                                                          (str name \. suffix))
                                                                        (gen/tuple (gen/fmap string/join
                                                                                             (gen/vector gen/char-alphanumeric 1 10))
                                                                                   (gen/elements ["com"
                                                                                                  "co.il"
                                                                                                  "ca"
                                                                                                  "co.uk"
                                                                                                  "com.sg"])))]]))
                      :zipcode           (gen/fmap str (gen/large-integer* {:min 10000 :max 99999}))})


(def profile-gen
  (gen'/map->hash-map profile-gen-map))

(def query-gen
  (let [make-foo (fn [gen] (gen/one-of [gen (gen/vector gen) (gen/set gen)]))]
    (gen/recursive-gen (fn [inner] (gen/one-of [(gen/let [op    (gen/elements [:and :or :not])
                                                          items (gen/not-empty (gen/vector inner))]
                                                         (cons op items))]))
                       (gen/let [ks (gen/not-empty (gen/vector-distinct (gen/elements (keys (dissoc profile-gen-map :id)))))
                                 vs (apply gen/tuple (map #(get profile-gen-map % (gen/return :whoops)) ks))]
                                (zipmap ks vs)))))

(defn normalize-query [query]
  (prewalk (fn [node]
             (cond
               (satisfies? u/Queryable node)
               (u/->query-expression node)
               (sequential? node) (cons (keyword (first node)) (rest node))
               :else node)) query))
(defn local-eval [population query]
  (let [population (set population)
        pop-ids (into #{} (map :id) population)]
    (letfn [(to-str [x]
              (cond
                (number? x) (str x)
                (or (symbol? x) (keyword? x)) (name x)
                :else (str x)))
            (test-item [k v pop]
              (when-let [pv (get pop k)]
                (if (sequential? pv)
                  (some #(= (to-str %) (to-str v)) pv)
                  (= (to-str pv) (to-str v)))))


            (eval-item [s]
              (case s
                "total" pop-ids
                "empty" #{}
                (do
                  (assert (string/includes? s ":") (str "weird key " s))
                  (let [[k v] (string/split s #":" 2)
                        k (keyword k)]
                    (into #{} (map :id) (filter #(test-item k v %) population))))))
            (eval-op [[op & args]]
              (assert (#{:and :or :not} op) (str "was actually " (pr-str op)))
              (if (= 0 (count args))
                (case (keyword (name op))
                  :or pop-ids
                  :and #{})
                (case (keyword (name op))
                  :and (apply set/intersection args)
                  :or (apply set/union args)
                  :not (apply set/difference (if (< (count args) 2)
                                               (cons pop-ids args)
                                               args)))))]
      (->> query
           (normalize-query)
           (postwalk (fn [node]
                       (cond
                         (sequential? node) (eval-op node)
                         (string? node) (eval-item node)
                         :else node)))

           (into #{} (map #(hash-map :id %)))))))

(defn local-eval [population query]
  (let [population (set population)
        pop-ids (into #{} (map :id) population)]
    (letfn [(to-str [x]
              (cond
                (number? x) (str x)
                (or (symbol? x) (keyword? x)) (name x)
                :else (str x)))
            (test-item [k v pop]
              (when-let [pv (get pop k)]
                (if (sequential? pv)
                  (some #(= (to-str %) (to-str v)) pv)
                  (= (to-str pv) (to-str v)))))


            (item->predicate [s]
              (case s
                "total" (constantly true)
                "empty" (constantly false)
                (do
                  (assert (string/includes? s ":") (str "weird key " s))
                  (let [[k v] (string/split s #":" 2)
                        k (keyword k)]
                    #(test-item k v %)))))
            (op->predicate [[op & args]]
              (assert (#{:and :or :not} op) (str "was actually " (pr-str op)))
              (if (= 0 (count args))
                (case (keyword (name op))
                  :or (constantly true)
                  :and (constantly false)
                  :not (throw (ex-info "'not' without arguments" {})))
                (case (keyword (name op))
                  :and (apply every-pred args)
                  :or (apply some-fn args)
                  :not (if (< (count args) 2)
                         (comp not (first args))
                         (apply every-pred (first args) (map #(comp not %)) (rest args))))))]
      (let [pred (->> query
                      (normalize-query)
                      (postwalk (fn [node]
                                  (cond
                                    (sequential? node) (op->predicate node)
                                    (string? node) (item->predicate node)
                                    :else node))))]
        (into #{} (comp (filter pred) (map :id)) population)))))

(defn bury-negations
  "Convert query to form with no :not nodes, and any negated leaves marked with ! prefix"
  [query]
  (letfn [(rewrite-not [expr]
            (let [x (cond
                      (#{"total" "empty"} expr)
                      ({"total" "empty" "empty" "total"} expr)
                      (and (string? expr) (string/starts-with? expr "!"))
                      (subs expr 1)
                      (string? expr)
                      (str \! expr)
                      (and (= (first expr) :not) (> (count expr) 2))
                      (apply list :or (rewrite-not (second expr)) (map rewrite (nnext expr)))
                      (= (first expr) :not)
                      [:and (second expr)]
                      (#{:and :or} (first expr))
                      (cons ({:and :or :or :and} (first expr)) (map rewrite-not (rest expr))))]
              x))

          (rewrite [expr]
            (let [x (cond (string? expr)
                          expr
                          (not (sequential? expr))
                          expr
                          (and (= (first expr) :not) (> (count expr) 2))
                          (apply list :and (rewrite (second expr)) (map rewrite-not (nnext expr)))
                          (= (first expr) :not) (rewrite-not (second expr))
                          :else
                          (cons (first expr) (map rewrite (rest expr))))]
              x))]

    (rewrite query)))

(defn satisfiable? [query]
  (let [vars (atom {})]
    (letfn [(get-var [name]
              (if-let [var (get @vars name)]
                var
                (let [v (l/lvar name)]
                  (swap! vars assoc name v)
                  v)))
            (item->predicate [s]
              (case s
                "total" l/succeed
                "empty" l/fail
                (do
                  (assert (string/includes? s ":") (str "weird key " s))
                  (let [[name target] (if (string/starts-with? s "!")
                                        [(subs s 1) false]
                                        [s true])]
                    (l/== (get-var name) target)))))
            (op->predicate [[op & args]]
              (assert (#{:and :or :not} op) (str "was actually " (pr-str op)))
              (case (count args)
                0 (case (keyword (name op))
                    :or l/succeed
                    :and l/fail)
                1 (first args)
                (case (keyword (name op))
                  :and (l/and* args)
                  :or (l/or* args))))
            (convert-query [query]
              (->> query
                   (normalize-query)
                   (bury-negations)
                   (postwalk (fn [node]
                               (cond
                                 (sequential? node) (op->predicate node)
                                 (string? node) (item->predicate node)
                                 :else node)))))]
      (seq (l/run-nc 1 [q]
                  (convert-query query)
                  (l/== q @vars))))))
(defn logic-same
  "Test if two queries are the same. Returns nil if they are, and a counter-example otherwise"
  [query1 query2]
  (satisfiable? [:and query1 [:not query2]]))

(def index-map
  {:gender :gender
   :age :age
   :income :income
   :cc :cc
   :frequent-keywords :f
   :uw :u
   :gsw :s
   :td :d
   :race :race
   :zipcode :z})
(defn profile-indexer [obj]
  (vec (for [[key index] index-map]
         [index (get obj key)])))