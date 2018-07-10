(defproject com.amobee/meeseeks-db "0.10.0"
  :description "Redis-based fast sharded set-oriented DB"
  :url "https://github.com/kontera-technologies/meeseeks-db"

  :dependencies [[org.clojure/clojure     "1.9.0"]
                 [prismatic/schema        "1.1.9"]
                 [com.taoensso/carmine    "2.14.0"]
                 [com.taoensso/nippy      "2.12.2"]
                 [clj-time                "0.14.3"]
                 [org.clojure/core.async  "0.4.474"]]
  :license {:name "GNU Lesser General Public License - v3"
            :url    "https://www.gnu.org/licenses/lgpl-3.0.en.html"}
  :repl-options {:init-ns user
                 :init    (set! *print-length* 100)}

  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/tools.nrepl "0.2.12"]
                                  [org.clojure/test.check  "0.9.0"]]}
             :repl {:plugins [[com.billpiel/sayid "0.0.10"]]}})