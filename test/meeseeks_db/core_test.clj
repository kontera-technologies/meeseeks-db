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
            [clojure.test.check :as tc]
            [schema.test :refer [validate-schemas]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.string :refer [join]]
            [clojure.set :refer [union difference intersection]]
            [meeseeks-db.test-db :refer [initialize-client index-entities!]]
            [meeseeks-db.query :as q]
            [meeseeks-db.cursor :as cursor])
  (:import [java.util.regex Pattern]))

;; utils

(defn prop-is? [k v o]
  (= v (get o k)))

(defn prop-has? [k v o]
  (boolean ((set (get o k)) v)))

(defn prop-visited? [domain o]
  (prop-has? :td domain o))

;; generators

(def property-gen
  (gen/fmap (fn [[id datatype label description]]
              {:id          id
               :datatype    datatype
               :label       label
               :description description})
            (gen/tuple (gen/fmap (fn [id] (str "P" id)) gen/pos-int)
                       (gen/elements [:string :quantity :globe-coordinate :wikimedia-item :time])
                       (gen/not-empty gen/string-alphanumeric)
                       gen/string-alphanumeric)))

(def profile-gen
  (gen/hash-map
    :id                gen/uuid
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
                                                       (gen/tuple (gen/fmap join
                                                                            (gen/vector gen/char-alphanumeric 1 10))
                                                                  (gen/elements ["com"
                                                                                 "co.il"
                                                                                 "ca"
                                                                                 "co.uk"
                                                                                 "com.sg"])))]]))
     :zipcode           (gen/fmap str (gen/large-integer* {:min 10000 :max 99999}))))
(clojure.test/use-fixtures :once validate-schemas)
(defn profile-indexer [obj]
  [[:gender (:gender obj)]
   [:age (:age obj)]
   [:income (:income obj)]
   [:cc (:cc obj)]
   [:f (:frequent-keywords obj)]
   [:u (:uw obj)]
   [:s (:gsw obj)]
   [:d (:td obj)]
   [:race (:race obj)]
   [:z (:zipcode obj)]])


(deftest simple
  (let [client   (initialize-client profile-indexer)
        profiles (gen/sample profile-gen 100)]
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
        (is (and (some #{"yahoo.com"} (:td profile))
                 (not (some #{"google.com"} (:td profile)))))))))

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

(deftest cursors
  (let [client   (initialize-client profile-indexer)
        profiles (gen/sample profile-gen 10)]
    (index-entities! client profiles)
    (with-open [cursor (cursor/create-cursor! client
                                              (q/compile-query {:gender :male}))]
      (is (= (-> (cursor/cursor-seq cursor)
                 sort)
             (->> profiles
                  (filter #(= (:gender %) :male))
                  (map :id)
                  distinct
                  sort))))))

(defn similar-queries? [expected actual]
  (let [expected-name (:name expected)
        actual-name   (:name actual)]
    (and
      (if (and (instance? Pattern expected-name) (string? actual-name))
        (is (boolean (re-matches expected-name actual-name)))
        (is (= expected-name actual-name)))
      (every? true? (map similar-queries? (:nested expected) (:nested actual))))))

(deftest compile-query
  (testing "simple"
    (is (similar-queries? {:name "gender:male"}
                          (q/compile-query [:and "gender:male"])))
    (is (similar-queries? {:name   #"^tmp:.*$"
                           :op     :and
                           :nested [{:name   #"^tmp:.*$"
                                     :op     :or
                                     :nested [{:name "d:google.com"}
                                              {:name "d:yahoo.com"}
                                              {:name "d:bing.com"}]}
                                    {:name "gender:male"}
                                    {:name   #"^tmp:.*$"
                                     :op     :or
                                     :nested [{:name "age:2-11"}
                                              {:name "age:12-17"}]}]}
                          (q/compile-query {:d      ["google.com"
                                                          "yahoo.com"
                                                          "bing.com"]
                                            :gender :male
                                            :age    ["2-11"
                                                     "12-17"]})))))

(deftest query->cursor
  (let [client   (initialize-client profile-indexer)
        profiles (gen/sample profile-gen 100)]
    (index-entities! client profiles)
    (testing "all males"
      (let [expected-profiles (filter #(= (:gender %) :male) profiles)
            query             (q/compile-query {"gender" "male"})]
        (is (= (count expected-profiles) (q/query->cursor client query)))
        (#'cursor/cleanup-query-cursor (:db client) query)))
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
               (q/query->cursor client query)))
        (#'cursor/cleanup-query-cursor (:db client) query)))))

(deftest map->query-expr
  (testing "simple"
    (is '(and "countries:US")
        (q/map->query-expr {:countries ["US"]})))
  (testing "simple and"
    (is '(and "countries:US" "frequent-keywords:5101")
        (q/map->query-expr {:countries         ["US"]
                             :frequent-keywords [5101]})))
  (testing "simple and with or"
    (is '(and "countries:US" (or "frequent-keywords:5101" "frequent-keywords:5102"))
        (q/map->query-expr {:countries         ["US"]
                             :frequent-keywords [5101 5102]})))
  (testing "and with or for different keys"
    (is '(and "countries:US" (or "frequent-keywords:5101" "gsw:5101"))
        (q/map->query-expr {:countries                ["US"]
                             [:frequent-keywords :gsw] [5101]})))
  (testing "simple not"
    (is '(and (not "total" "d:yahoo.com"))
        (q/map->query-expr {:d ["!yahoo.com"]})))
  (testing "more complex not"
    (is '(and "countries:US"
              (not (or "d:yahoo.com" "d:bing.com"
                       (and "d:duckduckgo.com" "d:facebook.com"))
                   "google.com"))
        (q/map->query-expr {:countries ["US"]
                             :d         ["yahoo.com" "!google.com" "bing.com"
                                         "duckduckgo.com" "facebook.com"]}))))

(deftest queries
  (let [client   (initialize-client profile-indexer)
        profiles (gen/sample profile-gen 1000)]
    (index-entities! client profiles)
    (testing "simple query"
      (let [cursor-spec (q/compile-query '(and "cc:us"))
            filtered    (filter #(= "us" (:cc %)) profiles)]
        (try
          (is (= (count filtered) (q/query->cursor client cursor-spec)))
          (finally
            (q/delete-cursor client cursor-spec)))))
    (testing "simple and query"
      (let [cursor-spec (q/compile-query '(and "cc:us" "gender:male"))
            filtered    (filter #(and (= "us" (:cc %)) (= :male (:gender %))) profiles)]
        (try
          (is (= (count filtered) (q/query->cursor client cursor-spec)))
          (finally
            (q/delete-cursor client cursor-spec)))))
    (testing "simple and with or query"
      (let [cursor-spec (q/compile-query '(and "cc:us" (or "f:1" "s:1")))
            filtered    (filter #(and (= "us" (:cc %))
                                      (or (contains? (set (:frequent-keywords %)) 1)
                                          (contains? (set (:gsw %)) 1)))
                                profiles)]
        (try
          (is (= (count filtered) (q/query->cursor client cursor-spec)))
          (finally
            (q/delete-cursor client cursor-spec)))))
    (testing "simple not query"
      (let [cursor-spec (q/compile-query '(and (not "total" "d:yahoo.com")))
            filtered    (remove #(contains? (set (:td %)) "yahoo.com") profiles)]
        (try
          (is (= (count filtered) (q/query->cursor client cursor-spec)))
          (finally
            (q/delete-cursor client cursor-spec)))))
    (testing "single term not queries"
      (let [cursor-spec (q/compile-query '(not "d:yahoo.com"))
            filtered    (remove #(contains? (set (:td %)) "yahoo.com") profiles)]
        (try
          (is (= (count filtered) (q/query->cursor client cursor-spec)))
          (finally
            (q/delete-cursor client cursor-spec)))))
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
          (is (= (count filtered) (q/query->cursor client cursor-spec)))
          (finally
            (q/delete-cursor client cursor-spec)))))))

(comment ;Tests broken
 (deftest scoped-queries
  (let [client   (initialize-client profile-indexer)
        profiles (gen/sample profile-gen 1000)]
    (index-entities! client profiles)
    (testing "simple scoped query"
      (let [scope-spec   (q/compile-query '(and "d:yahoo.com"))
            query-m-spec (->> '(and "gender:male")
                              q/compile-query
                              (q/apply-scope scope-spec))
            query-f-spec (->> '(and "gender:female")
                              q/compile-query
                              (q/apply-scope scope-spec))
            filtered-m   (intersection
                           (set (filter #(prop-visited? "yahoo.com" %) profiles))
                           (set (filter #(prop-is? :gender :male %) profiles)))
            filtered-f   (intersection
                           (set (filter #(prop-visited? "yahoo.com" %) profiles))
                           (set (filter #(prop-is? :gender :female %) profiles)))]
        (try
          (q/query->cursor client scope-spec)
          (is (= (count filtered-m)
                 (q/query->cursor client query-m-spec)))
          (is (= (count filtered-f)
                 (q/query->cursor client query-f-spec)))
          (finally
            (q/delete-cursor client query-m-spec)
            (q/delete-cursor client query-f-spec)
            (q/delete-cursor client scope-spec)))))
    (testing "scoped query with complex scope"
      (let [scope-spec   (q/compile-query '(or "d:yahoo.com"
                                               "d:bing.com"))
            query-m-spec (->> '(and "cc:us" "gender:male")
                              q/compile-query
                              (q/apply-scope scope-spec))
            query-f-spec (->> '(and "cc:us" "gender:female")
                              q/compile-query
                              (q/apply-scope scope-spec))
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
          (q/query->cursor client scope-spec)
          (is (= (count filtered-m)
                 (q/query->cursor client query-m-spec)))
          (is (= (count filtered-f)
                 (q/query->cursor client query-f-spec)))
          (finally
            (q/delete-cursor client query-m-spec)
            (q/delete-cursor client query-f-spec)
            (q/delete-cursor client scope-spec)))))
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
                         '((and "cc:us" "gender:male"
                            (and "cc:us" "gender:female"))))]
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
                         '((and "cc:us" "gender:male"
                            (and "cc:us" "gender:female"))))]
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
                          '((and "cc:us" "gender:male"
                             (and "cc:us" "gender:female")
                             (and "cc:us" "gender:male" "race:black")
                             (and "cc:us" "gender:female" "race:black")))
                          '(and "cc:us" "race:white"))]
        (is (= -1 (:size (nth results 0))))
        (is (= -1 (:size (nth results 1))))
        (is (= (count filtered-mb) (:size (nth results 2))))
        (is (= (count filtered-fb) (:size (nth results 3)))))))))

(deftest smart-bulk-bug
  (let [client   (initialize-client profile-indexer)
        profiles (gen/sample profile-gen 300)]
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
        (is (every? (fn [[r sr]]
                      (and (= expected-count (:size r))
                           (or (= -1 (:size sr))
                               (= (:size r) (:size sr)))))
                    (map vector rs srs)))))))
