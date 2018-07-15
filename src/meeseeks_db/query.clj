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

(ns meeseeks-db.query
  (:require
    [taoensso.carmine :as car :refer [wcar]]
    [clojure.core.async :as async :refer [>!! <!!]]
    [clojure.stacktrace :as st]
    [schema.core :as s]
    [clojure.string :refer [starts-with? replace-first]]
    [meeseeks-db.utils :refer [stringify attr translate-iids fetch-objects run-command max-workers
                               ;;Schemas
                               Key Value Op Attr Named QueryMap QueryExpression]]))

(s/defrecord Query [name :- Named
                    transient? :- s/Bool
                    op :- (s/maybe (s/enum :and :or :not))
                    nested :- [(s/maybe (s/recursive (ref Query)))]])

(s/defn ^:private query-node :- Query
  [op :- Op
   nested :- [Query]]
  (->Query (str "tmp:" (name op) \_ (hash (into #{} (map (comp name :name)) nested)))
           true
           (keyword op)
           (vec nested)))


(s/defn ^:private query-attr :- Query
  [name :- Attr]
  (->Query name
           false
           nil
           nil))
(defn- filter-value->query [k v]
  (cond
    (vector? v) (cons :or (map #(filter-value->query k %) v))
    (set? v) (cons :and (map #(filter-value->query k %) v))
    :else (attr k v)))



(defn- filter->query
  ([[ks vs]]
   (filter->query ks vs))
  ([ks vs]
   (cond
     (set? ks) (cons :and (map #(filter->query % vs) ks))
     (sequential? ks) (cons :or (map #(filter->query % vs) ks))
     :else (let [vs             (if (coll? vs) vs [vs])
                 op             (if (set? vs) :and :or)
                 negated?       #(and (string? %) (starts-with? % "!"))
                 c->q           #(filter-value->query ks %)
                 negated->query #(c->q (replace-first % #"^\!" ""))]
             (if (> (count vs) 1)
               (let [{:keys [include exclude]} (group-by #(if (negated? %) :exclude :include) vs)]
                 (if (seq exclude)
                   (cons :not
                         (case (count include)
                           0 (apply list "total" (map negated->query exclude))
                           1 (apply list (c->q (first include)) (map negated->query exclude))
                           (apply list (cons op (map c->q include)) (map negated->query exclude))))
                   (cons op (map c->q vs))))
               (if (negated? (first vs))
                 (list :not "total" (negated->query (first vs)))
                 (c->q (first vs))))))))

(comment
  (map->query-expr {:a 1 [:b :c] [2 3] :x ["a" :!b]})
  :=>
  (:and "a:1" (:or "b:2" "b:3" "c:2" "c:3" (:not "x:a" "x:b")))) ;; :not means set difference!

(s/defn map->query-expr :- QueryExpression
  [filters :- QueryMap]
  "Convert map to query expression

  "
  (->> filters
       (map filter->query)
       (cons :and)))

(s/defn ^:private simplify :- QueryExpression
  [expr :- QueryExpression]
  (loop [expr expr]
    (cond
      (sequential? expr)
      (let [op   (keyword (first expr))
            args (map simplify (rest expr))]
        (cond (and (not= op :not) (= 1 (count args)))
              (recur (first args))
              (and (not= op :not) (some #(and (sequential? %) (= (keyword (first %)) op)) args))
              (recur (apply list op (mapcat #(if (and (sequential? %) (= (keyword (first %)) op)) (rest %) [%]) args)))
              :else
              (apply list op args)))
      (associative? expr)
      (recur (map->query-expr expr))
      :else
      expr)))

(defn- compile-query* [q]
  (cond
    (sequential? q) (let [[op & args] q]
                      (if (= 0 (count args))
                        (query-attr "total")
                        (query-node op (map compile-query* args))))
    (associative? q) (assert "This should never happen: maps should have been simplified")
    :else (query-attr (or q "total"))))

(s/defn compile-query :- Query
  [q :- QueryExpression]
  (if (instance? Query q)
    q
    (compile-query* (simplify q))))


(defn apply-scope [scope query]
  (let [pscope (query-attr (:name scope))]
    (if (= (:op query) :and)
      (update-in query [:nested] conj pscope)
      (query-node :and [pscope query]))))

(defn- run-query* [query conn]
  (try
    (letfn [(execute [query]
              (let [{:keys [op name nested]} query]
                (doseq [q nested]
                  (execute q))
                (if (seq nested)
                  (let [to-del (map :name (filter :transient? nested))]
                    (condp = op
                      :and (apply car/sinterstore name (map :name nested))
                      :or (apply car/sunionstore name (map :name nested))
                      :not (apply car/sdiffstore name (if (= 1 (count nested))
                                                        (into ["total"] (map :name nested))
                                                        (map :name nested))))
                    (when (seq to-del)
                      (apply car/del to-del)
                      (car/scard name)))
                  (car/scard name))))]
      (let [res (wcar conn (execute query))]
        (if (number? res) res (last res))))
    (catch Exception ex
      (locking *out*
        (println
          (format "failed to run [%s] on [%s]: %s"
                  query conn
                  (with-out-str
                    (st/print-stack-trace ex)))))
      -1)))

(s/defn run-query! :- s/Int [client query :- Query]
  "Creates a cursor and returns the size of the resulting query"
  (run-command @(:db client)
               (partial run-query* query)
               (fnil + 0 0) 0
               (fn [ex]
                 (locking *out*
                   (st/print-stack-trace ex))
                 0)))

(defn query->command-list [query]
  (letfn [(execute [query]
            (let [{:keys [op name nested]} query]
              (doseq [q nested]
                (execute q))
              (when (seq nested)
                (let [to-del (map :name (filter :transient? nested))]
                  (condp = op
                    :and (apply car/sinterstore name (map :name nested))
                    :or (apply car/sunionstore name (map :name nested))
                    :not (apply car/sdiffstore name (map :name nested)))
                  (when (seq to-del)
                    (apply car/del to-del))))))]
    (execute query)))

(defn multiple-queries->cursor [client queries]
  (let [queries (map-indexed #(assoc %2 :id %1) queries)
        results (doall (pmap (fn [connection]
                               (let [res (wcar connection
                                               (doseq [q queries] (query->command-list q))
                                               (doseq [n (map #(:name %) queries)] (car/scard n)))
                                     res (take-last (count queries) res)
                                     res (map vector queries res)]
                                 res))

                             @(:db client)))
        results (apply concat results)
        results (reduce (fn [acc [query size]]
                          (update-in acc [query] (fnil + 0 0) size)) {} results)]
    (map results queries)))


(defn cleanup-query [client query]
  (when (:transient? query)
    (run-command @(:db client)
                 #(wcar % (car/del (:name query)))
                 conj []
                 (fn [ex]
                   (locking *out*
                     (st/print-stack-trace ex))
                   false))))

(defn stats* [scope scope-sizes reference reference-sizes conn s-query r-query]
  (try
    (let [size-f         (fn [cache conn query]
                           (if (contains? @cache conn)
                             (get @cache conn)
                             (let [size (run-query* query conn)]
                               (swap! cache assoc conn size)
                               size)))
          scope-size     (size-f scope-sizes conn scope)
          reference-size (size-f reference-sizes conn reference)
          s-query-size   (run-query* s-query conn)
          r-query-size   (run-query* r-query conn)
          g0             (* 1.00 (/ s-query-size (max scope-size 1)))
          g1             (* 1.25 (/ r-query-size (max reference-size 1)))]
      [s-query conn s-query-size scope-size r-query-size reference-size (> g0 g1)])
    (catch Exception ex
      (locking *out*
        (st/print-stack-trace ex))
      [s-query conn -1 -1 -1 -1 false])))

(defn run-stats [jobs scope reference]
  (let [in-ch           (async/chan)
        out-chs         (for [_ (range (min (count jobs) max-workers))]
                          (async/chan))
        scope-sizes     (atom {})
        reference-sizes (atom {})]
    (doall
      (for [out-ch out-chs]
        (async/thread
          (loop []
            (when-some [[conn s-query r-query] (<!! in-ch)]
              (>!! out-ch (stats* scope scope-sizes
                                  reference reference-sizes
                                  conn s-query r-query))
              (recur)))
          (async/close! out-ch)
          :done)))
    (async/onto-chan in-ch jobs)
    (async/merge out-chs)))

(defn bulk-scoped-queries [client scope-spec queries-specs]
  (let [scope   (compile-query scope-spec)
        queries (->> queries-specs
                     (map compile-query)
                     (map (partial apply-scope scope)))]
    (try
      (run-query! client scope)
      (doall
        (map #(hash-map :size %)
             (multiple-queries->cursor client queries)))
      (finally
        (doall (pmap #(cleanup-query client %) (conj queries scope)))))))

(defn bulk-scoped-smarter-queries [client scope-spec queries-specs reference-spec]
  (let [dbs @(:db client)]
    (if (<= (count queries-specs) (count dbs))
      (bulk-scoped-queries client scope-spec queries-specs)
      (let [scope      (compile-query scope-spec)
            reference  (compile-query reference-spec)
            queries    (map compile-query queries-specs)
            s-queries  (->> queries
                            (map (partial apply-scope scope))
                            (map-indexed #(assoc %2 :id %1)))
            r-queries  (->> queries
                            (map (partial apply-scope reference))
                            (map-indexed #(assoc %2 :id %1)))
            stats-jobs (map vector (cycle dbs) s-queries r-queries)]
        (try
          (let [stats-ch (run-stats (shuffle stats-jobs) scope reference)
                skip-ch  (async/chan)
                in-ch    (async/chan)
                out-chs  (for [_ (range (min max-workers
                                             (- (* (count dbs)
                                                   (count s-queries))
                                                (count stats-jobs))))]
                           (async/chan))]
            (async/thread
              (loop []
                (when-some [[q conn s-query-size _ _ _ good?] (<!! stats-ch)]
                  (if good?
                    (let [jobs (map vector (remove #(= % conn) dbs) (repeat q))]
                      (doseq [j jobs] (>!! in-ch j))
                      (>!! skip-ch [q s-query-size]))
                    (>!! skip-ch [q -1]))
                  (recur)))
              (async/close! in-ch)
              (async/close! skip-ch))
            (doall
              (for [out-ch out-chs]
                (async/thread
                  (loop []
                    (when-some [[conn q] (<!! in-ch)]
                      (>!! out-ch [q (run-query* q conn)])
                      (recur)))
                  (async/close! out-ch))))
            (let [results (<!! (async/reduce
                                 (fn [acc [query size]]
                                   (if (neg? size)
                                     (assoc acc query size)
                                     (update-in acc [query] (fnil + 0 0) size)))
                                 {}
                                 (async/merge (conj out-chs skip-ch))))]
              (map #(hash-map :size (get results %)) s-queries)))
          (finally
            (doseq [q (concat s-queries r-queries [scope reference])]
              (cleanup-query client q))))))))

