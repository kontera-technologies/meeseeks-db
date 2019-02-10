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

(ns meeseeks-db.core-test
  (:require [meeseeks-db.core :as sut]
            [clojure.test :refer [deftest testing is]]
            [midje.sweet :refer [facts fact =>] :as m]
            [taoensso.carmine :as car]
            [schema.test :refer [validate-schemas]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :as tct]
            [clojure.walk :refer [postwalk prewalk]]
            [clojure.string :refer [join]]
            [clojure.set :refer [union difference intersection]]
            [meeseeks-db.test-db :refer [initialize-client
                                         index-entities!
                                         sample
                                         local-eval
                                         profile-gen
                                         property-gen
                                         node
                                         attr
                                         prop-is?
                                         prop-visited?
                                         profile-indexer
                                         query-gen]]
            [meeseeks-db.query :as q]
            [meeseeks-db.utils :as u]
            [meeseeks-db.cursor :as cursor]))

(clojure.test/use-fixtures :once validate-schemas)

(deftest simple
  (let [client   (initialize-client profile-indexer)
        profiles (sample profile-gen 100)]
    (index-entities! client profiles)
    (testing "all profiles"
      (is (= (count (distinct (map :id profiles)))
             (:size (sut/query client "total" 0)))))
    (testing "simple query"
      (let [filtered (filter #(and (= (:gender %) :male)
                                   (or (= (:age %) :2-11) (= (:age %) :12-17)))
                             profiles)]
        (is (= (count (distinct (map :id filtered)))
               (:size (sut/query client {:gender [:male]
                                          :age    [:2-11 :12-17]}))))))
    (testing "simple query + fetch"
      (let [filtered (filter #(and (= (:gender %) :male)
                                   (= (:age %) :25-34)
                                   (let [domains (set (:td %))]
                                     (and (contains? domains "yahoo.com")
                                          (contains? domains "google.com"))))
                             profiles)
            result   (sut/query client [:and
                                        {:gender [:male]
                                         :age    [:25-34]
                                         :d #{"yahoo.com" "google.com"}}]
                                (* 2 (count filtered))
                                [:gender :id])]
        (when-not (is (= (count (distinct (map :id filtered))) (:size result)))
          (print "Expected:" (map :id filtered) "\nActual:" (map :id (:sample result))))
        (when (not-empty filtered)
          ;(is (= (count (:sample result)) 1))
          (let [sampled (first (:sample result))
                filtered-ids (set (map :id filtered))]
            (is (contains? filtered-ids (:id sampled)))))))
    (testing "fetching with fields"
      (let [p       (last profiles)
            fetched (sut/fetch client (:id p) [:id :gender :age :race :income])]
        (is (= (:id p) (:id fetched)))
        (is (= (:gender p) (keyword (:gender fetched))))
        (is (= (:age p) (keyword (:age fetched))))
        (is (= (:race p) (keyword (:race fetched))))
        (is (= (:income p) (keyword (:income fetched))))))
    (testing "not filter"
      (let [res     (sut/query client {:gender [:male]
                                       :d      ["yahoo.com" "!google.com"]}
                               1 [:td])
            profile (first (:sample res))]
        (and

          (is (> (:size res) 0))
          (is (some? profile))
          (is (some? (:td profile)))
          (is (some #{"yahoo.com"} (:td profile)))
          (is (not (some #{"google.com"} (:td profile)))))))))

(deftest no-indices
  (let [client   (initialize-client (fn [_obj] nil))
        property (gen/generate property-gen)]
    (sut/index! client property)
    (is (= (:id property) (:id (sut/fetch client (:id property)))))))

(deftest custom-indices
  (let [client (initialize-client (fn [x]
                                    (select-keys x [:subject-keywords :labels])))
        s1     {:text             "I'm an evil string"
                :subject-keywords [111, 222, 333]
                :object-keywords  [777, 888, 999]
                :sentiment        {:pos 0.1, :neu 0.2, :neg 0.7}
                :labels           ["neg"]}
        s2     {:text             "I'm a very evil string"
                :subject-keywords [111, 222, 333]
                :object-keywords  [777, 888, 999]
                :sentiment        {:pos 0.1, :neu 0.2, :neg 0.7}
                :labels           ["neg"]}]
    (sut/index! client (assoc s1 :id "Z"))
    (sut/index! client (assoc s2 :id "Z2"))
    (let [result (sut/query client "total" 1 [:id])]
      (is (= 2 (:size result)))
      (is (#{"Z" "Z2"} (:id (first (:sample result))))))
   (sut/unindex! client "Z")
   (let [result (sut/query client "total" 1 [:id])]
     (is (= 1 (:size result)))
     (is (= "Z2" (:id (first (:sample result))))))))

(facts "about cursors"
  (let [client   (initialize-client profile-indexer)
        profiles (map-indexed #(assoc %2 :id (str %1)) (sample profile-gen 1000))

        us-males (->> profiles
                      (filter #(and (= (:gender %) :male) (= (:cc %) "us")))
                      (map :id)
                      distinct)
        query    {:gender :male :cc "us"}]
    (index-entities! client profiles)

    (let [key-name (with-open [cursor (cursor/create-cursor! client (q/compile-query query))]
                     (fact "create-cursor! works with a compiled query"
                           (cursor/cursor-seq cursor) => (m/just us-males :in-any-order))
                     (get-in cursor [:query :name]))]
      (fact "Cursor cleans up after itself"
            (u/run-command @(:db client) #(car/wcar % (car/exists key-name)) + 0)
            => 0))
    (fact "create-cursor! works with a query expression"
          (with-open [cursor (cursor/create-cursor! client
                                                    query)]
            (cursor/cursor-seq cursor) => (m/just us-males :in-any-order)))
    (when (pos? (count us-males))
      (let [q (sut/query client query (count profiles))]
        (fact "Samples have the right size"
              (count (:sample q)) => (count us-males)
              (:size q) => (count us-males)))

      (facts "when unindexing"
        (doseq [id (take 10 us-males)]
          (sut/unindex! client id))
        (let [new-male-count (max 0 (- (count us-males) 10))
              new-total-count (- (count profiles) (min 10 (count us-males)))
              total           (sut/query client :total (count profiles))
              q               (sut/query client query (count profiles))]
          (fact "Reported size shrinks"
                (:size total) => new-total-count
                (:size q) => new-male-count)
          (fact "Sample size shrinks"
                (count (:sample total)) => new-total-count
                (count (:sample q)) => new-male-count))))

    (fact "remove-all works"
          (sut/remove-all! client)
          (sut/query client :total (count profiles)) => {:size 0 :sample []})))

(facts "about map->query-expr"
  (q/map->query-expr {:countries ["US"]}) => '(:and "countries:US")
  (q/map->query-expr {:countries "US"}) => '(:and "countries:US")
  (q/map->query-expr {"countries" [:US]}) => '(:and "countries:US")
  (q/map->query-expr {'countries ['US]}) => '(:and "countries:US")
  (fact "AND different fields"
        (q/map->query-expr {:countries         ["US"]
                            :frequent-keywords [5101]}) => '(:and "countries:US" "frequent-keywords:5101"))
  (fact "OR different values for the same field"
        (q/map->query-expr {:countries         ["US"]
                            :frequent-keywords [5101 5102]}) => '(:and "countries:US" (:or "frequent-keywords:5101" "frequent-keywords:5102")))
  (fact "OR different fields with the same values"
        (q/map->query-expr {:countries                ["US"]
                            [:frequent-keywords :gsw] [5101]}) => '(:and "countries:US" (:or "frequent-keywords:5101" "gsw:5101")))
  (fact "NOT as complement"
        (q/map->query-expr {:d ["!yahoo.com"]}) => '(:and (:not "total" "d:yahoo.com")))
  (fact "NOT as set difference"
        (q/map->query-expr {:countries ["US"]
                            :d         ["yahoo.com" "!google.com" "bing.com"
                                        #{"duckduckgo.com" "facebook.com"}]}) => '(:and "countries:US"
                                                                                    (:not (:or "d:yahoo.com" "d:bing.com"
                                                                                              (:and "d:duckduckgo.com" "d:facebook.com"))
                                                                                         "d:google.com"))))
(m/facts :simple "about simplify"
  (#'q/simplify ["and", ["and", {"iw" 31565330}, ["not", {"iw" 1255650}]], {"cc" "us"}]) =>
  '(:not (:and "iw:31565330" "cc:us") "iw:1255650")
  (#'q/simplify ["and", ["and", {"iw" 31565330}, ["not", {"iw" 1255650}]], {"cc" []}]) =>
  '(:not "iw:31565330" "iw:1255650"))

(m/facts :simple "about compile-query"
  (fact "simple things work"
        (q/compile-query "gender:male") => (attr "gender:male")
        (fact "simplifying works"
              (q/compile-query [:and "gender:male"]) => (attr "gender:male")
              (q/compile-query [:or "gender:male"]) => (attr "gender:male")
              (q/compile-query [:or "cc:us" [:or "cc:uk" "cc:il"]]) => (node :or
                                                                             (attr "cc:us")
                                                                             (attr "cc:uk")
                                                                             (attr "cc:il")))
        (q/compile-query {:gender :male}) => (attr "gender:male")

        (q/compile-query [:or "gender:male" "gender:female"]) => (node :or
                                                                       (attr "gender:male")
                                                                       (attr "gender:female"))
        (q/compile-query {:d      ["google.com"
                                   "yahoo.com"
                                   "bing.com"]
                          :gender :male
                          :age    ["2-11"
                                   "12-17"]}) => (node :and
                                                       (node :or
                                                             (attr "d:google.com")
                                                             (attr "d:yahoo.com")
                                                             (attr "d:bing.com"))
                                                       (attr "gender:male")
                                                       (node :or
                                                             (attr "age:2-11")
                                                             (attr "age:12-17")))))


(deftest run-query
  (let [client   (initialize-client profile-indexer)
        profiles (sample profile-gen 100)]
    (index-entities! client profiles)
    (testing "all males"
      (let [expected-profiles (filter #(= (:gender %) :male) profiles)
            query             (q/compile-query {"gender" "male"})]
        (is (= (count expected-profiles) (q/run-query! client query)))
        (q/cleanup-query client query)))
    (testing "complex"
      (let [expected-profiles (filter (fn [p]
                                        (and (some #{"google.com"
                                                     "yahoo.com"
                                                     "bing.com"} (:td p))
                                             (= (:gender p) :male)
                                             (#{:2-11 :12-17} (:age p))))
                                      profiles)
            query             (q/compile-query [:and [:or "d:google.com"
                                                      "d:yahoo.com"
                                                      "d:bing.com"]
                                                "gender:male"
                                                [:or "age:2-11"
                                                 "age:12-17"]])]
        (is (= (count expected-profiles)
               (q/run-query! client query)))
        (q/cleanup-query client query)))))



(deftest queries
  (let [client   (initialize-client profile-indexer)
        profiles (sample profile-gen 1000)]
    (index-entities! client profiles)
    (testing "simple query"
      (let [cursor-spec (q/compile-query '(and "cc:us"))
            filtered    (filter #(= "us" (:cc %)) profiles)]
        (try
          (is (= (count filtered) (q/run-query! client cursor-spec)))
          (finally
            (q/cleanup-query client cursor-spec)))))
    (testing "simple and query"
      (let [cursor-spec (q/compile-query '(and "cc:us" "gender:male"))
            filtered    (filter #(and (= "us" (:cc %)) (= :male (:gender %))) profiles)]
        (try
          (is (= (count filtered) (q/run-query! client cursor-spec)))
          (finally
            (q/cleanup-query client cursor-spec)))))
    (testing "simple and with or query"
      (let [cursor-spec (q/compile-query '(and "cc:us" (or "f:1" "s:1")))
            filtered    (filter #(and (= "us" (:cc %))
                                      (or (contains? (set (:frequent-keywords %)) 1)
                                          (contains? (set (:gsw %)) 1)))
                                profiles)]
        (try
          (is (= (count filtered) (q/run-query! client cursor-spec)))
          (finally
            (q/cleanup-query client cursor-spec)))))
    (testing "simple not query"
      (let [cursor-spec (q/compile-query '(and (not "total" "d:yahoo.com")))
            filtered    (remove #(contains? (set (:td %)) "yahoo.com") profiles)]
        (try
          (is (= (count filtered) (q/run-query! client cursor-spec)))
          (finally
            (q/cleanup-query client cursor-spec)))))
    (testing "single term not queries"
      (let [cursor-spec (q/compile-query '(not "d:yahoo.com"))
            filtered    (remove #(contains? (set (:td %)) "yahoo.com") profiles)]
        (try
          (is (= (count filtered) (q/run-query! client cursor-spec)))
          (finally
            (q/cleanup-query client cursor-spec)))))
    (testing "more complex not query"
      (let [cursor-spec (q/compile-query '(and "cc:us"
                                               (not (or "d:yahoo.com"
                                                        "d:bing.com"
                                                        (and "d:duckduckgo.com"
                                                             "d:facebook.com"))
                                                    "d:google.com")))
            filtered    (intersection
                          (set (filter #(prop-is? :cc "us" %) profiles))
                          (difference
                             (union (set (filter #(or (prop-visited? "yahoo.com" %)
                                                      (prop-visited? "bing.com" %))
                                                  profiles))
                                    (set (filter #(and (prop-visited? "duckduckgo.com" %)
                                                       (prop-visited? "facebook.com" %))
                                                  profiles)))
                             (set (filter #(prop-visited? "google.com" %) profiles))))]
        (try
          (is (= (count filtered) (q/run-query! client cursor-spec)))
          (finally
            (q/cleanup-query client cursor-spec)))))))


(deftest scoped-queries
  (let [client   (initialize-client profile-indexer)
        profiles (sample profile-gen 1000)]
    (index-entities! client profiles)
    (testing "simple scoped query"
      (let [scope-spec   (q/compile-query '(and "d:yahoo.com"))
            query-m-spec (->> '(and "gender:male")
                              q/compile-query
                              (#'q/apply-scope scope-spec))
            query-f-spec (->> '(and "gender:female")
                              q/compile-query
                              (#'q/apply-scope scope-spec))
            filtered-m   (intersection
                           (set (filter #(prop-visited? "yahoo.com" %) profiles))
                           (set (filter #(prop-is? :gender :male %) profiles)))
            filtered-f   (intersection
                           (set (filter #(prop-visited? "yahoo.com" %) profiles))
                           (set (filter #(prop-is? :gender :female %) profiles)))]
        (try
          (q/run-query! client scope-spec)
          (is (= (count filtered-m)
                 (q/run-query! client query-m-spec)))
          (is (= (count filtered-f)
                 (q/run-query! client query-f-spec)))
          (finally
            (q/cleanup-query client query-m-spec)
            (q/cleanup-query client query-f-spec)
            (q/cleanup-query client scope-spec)))))
    (testing "scoped query with complex scope"
      (let [scope-spec   (q/compile-query '(or "d:yahoo.com"
                                               "d:bing.com"))
            query-m-spec (->> '(and "cc:us" "gender:male")
                              q/compile-query
                              (#'q/apply-scope scope-spec))
            query-f-spec (->> '(and "cc:us" "gender:female")
                              q/compile-query
                              (#'q/apply-scope scope-spec))
            filtered-m   (intersection
                           (union
                             (set (filter #(prop-visited? "yahoo.com" %) profiles))
                             (set (filter #(prop-visited? "bing.com" %) profiles)))
                           (intersection
                             (set (filter #(prop-is? :cc "us" %) profiles))
                             (set (filter #(prop-is? :gender :male %) profiles))))
            filtered-f   (intersection
                           (union
                             (set (filter #(prop-visited? "yahoo.com" %) profiles))
                             (set (filter #(prop-visited? "bing.com" %) profiles)))
                           (intersection
                             (set (filter #(prop-is? :cc "us" %) profiles))
                             (set (filter #(prop-is? :gender :female %) profiles))))]
        (try
          (q/run-query! client scope-spec)
          (is (= (count filtered-m)
                 (q/run-query! client query-m-spec)))
          (is (= (count filtered-f)
                 (q/run-query! client query-f-spec)))
          (finally
            (q/cleanup-query client query-m-spec)
            (q/cleanup-query client query-f-spec)
            (q/cleanup-query client scope-spec)))))
    (testing "multiple scoped queries"
      (let [filtered-m (intersection
                         (union
                           (set (filter #(prop-visited? "yahoo.com" %) profiles))
                           (set (filter #(prop-visited? "bing.com" %) profiles)))
                         (intersection
                           (set (filter #(prop-is? :cc "us" %) profiles))
                           (set (filter #(prop-is? :gender :male %) profiles))))
            filtered-f (intersection
                         (union
                           (set (filter #(prop-visited? "yahoo.com" %) profiles))
                           (set (filter #(prop-visited? "bing.com" %) profiles)))
                         (intersection
                           (set (filter #(prop-is? :cc "us" %) profiles))
                           (set (filter #(prop-is? :gender :female %) profiles))))
            results    (q/bulk-scoped-queries
                         client
                         '(or "d:yahoo.com" "d:bing.com")
                         '((and "cc:us" "gender:male")
                           (and "cc:us" "gender:female")))]
        (is (= (count filtered-m) (:size (first results))))
        (is (= (count filtered-f) (:size (second results))))))
    (testing "multiple scoped queries with empty scope"
      (let [filtered-m (intersection
                         (set (filter #(prop-is? :cc "us" %) profiles))
                         (set (filter #(prop-is? :gender :male %) profiles)))
            filtered-f (intersection
                         (set (filter #(prop-is? :cc "us" %) profiles))
                         (set (filter #(prop-is? :gender :female %) profiles)))
            results    (q/bulk-scoped-queries
                         client
                         '(and)
                         '((and "cc:us" "gender:male")
                           (and "cc:us" "gender:female")))]
        (is (= (count filtered-m) (:size (first results))))
        (is (= (count filtered-f) (:size (second results))))))
    (testing "smart-multiple scoped queries"
      (let [filtered-mb (intersection
                          (union
                            (set (filter #(prop-visited? "yahoo.com" %) profiles))
                            (set (filter #(prop-visited? "bing.com" %) profiles)))
                          (intersection
                            (set (filter #(prop-is? :cc "us" %) profiles))
                            (set (filter #(prop-is? :gender :male %) profiles))
                            (set (filter #(prop-is? :race :black %) profiles))))
            filtered-fb (intersection
                          (union
                            (set (filter #(prop-visited? "yahoo.com" %) profiles))
                            (set (filter #(prop-visited? "bing.com" %) profiles)))
                          (intersection
                            (set (filter #(prop-is? :cc "us" %) profiles))
                            (set (filter #(prop-is? :gender :female %) profiles))
                            (set (filter #(prop-is? :race :black %) profiles))))
            results     (q/bulk-scoped-smarter-queries
                          client
                          '(or "d:yahoo.com" "d:bing.com")
                          '((and "cc:us" "gender:male")
                            (and "cc:us" "gender:female")
                            (and "cc:us" "gender:male" "race:black")
                            (and "cc:us" "gender:female" "race:black"))
                          '(and "cc:us" "race:white"))]
        (is (= -1 (:size (nth results 0))))
        (is (= -1 (:size (nth results 1))))
        (is (= (count filtered-mb) (:size (nth results 2))))
        (is (= (count filtered-fb) (:size (nth results 3))))))))

(deftest smart-bulk-bug
  (let [client   (initialize-client profile-indexer)
        profiles (sample profile-gen 300)]
    (index-entities! client profiles)
    (let [expected-count    (count
                              (intersection
                                 (intersection
                                  (set (filter #(prop-visited? "bing.com" %) profiles))
                                  (set (filter #(prop-is? :cc "us" %) profiles)))
                                 (set (filter #(prop-visited? "bing.com" %) profiles))))
          common-filters    '(and "d:bing.com" "cc:us")
          query             '(and "d:bing.com")
          reference-filters '(and "cc:us")]
      (doseq [i (range 10)
              :let [rs  (q/bulk-scoped-queries client common-filters
                                               (take i (repeat query)))
                    srs (q/bulk-scoped-smarter-queries client common-filters
                                                       (take i (repeat query))
                                                       reference-filters)]]
        (doseq [[r sr] (map vector rs srs)]
          (is (= expected-count (:size r)))
          (when-not (= -1 (:size sr))
            (is (= (:size r) (:size sr)))))))))
