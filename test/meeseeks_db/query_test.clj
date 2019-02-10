(ns meeseeks-db.query-test
  (:require [clojure.test :refer :all]
            [schema.test :refer [validate-schemas]]
            [meeseeks-db.core :as sut]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :refer [for-all]]
            [clojure.test.check.clojure-test :as tct]
            [meeseeks-db.test-db :refer [sample initialize-client profile-gen query-gen local-eval logic-same index-entities!]]
            [meeseeks-db.query :as q]
            [clojure.set :as set]))
(clojure.test/use-fixtures :once validate-schemas)

(tct/defspec simplify-works
  (let [profiles (sample profile-gen 100)]
    (for-all [query query-gen]
      (let [simplified (#'q/simplify query)]
        (is (= (local-eval profiles query)
               (local-eval profiles simplified)) "same set")

        (is (= nil (logic-same query simplified)) "SAT is happy")))))

(tct/defspec real-query-works-like-local 10
  (let [client   (initialize-client identity)
        profiles (sample profile-gen 1000)]
    (sut/remove-all! client)
    (index-entities! client profiles)
    (for-all [query query-gen]
      (let [local-results  (local-eval profiles query)
            remote-results (sut/query client query 1000)]
        (is (= (count local-results)
               (:size remote-results)) "woot")
        (is (= (count local-results)
               (count (:sample remote-results))))
        (is (= local-results
               (set (:sample remote-results))))))))