# meeseeks-db

![Meeseeks logo](doc/MeeseeksHQ.png)
Redis-based fast sharded set-oriented DB

This library exists to help answer two questions about your data:

* What is the size of ((A1 ⋂ A2) ⋃ A3) where A1, A2, and A3 are sets
* Take random sample of K elements from product of ((A1 ⋂ A2) ⋃ A3)

Concrete example:
Let's assume we have a 10m user profiles, each profile consists of following fields: gender (can be 'male' or 'female'), age (can be '2-11', '12-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+'), and keywords (set of keyword ids this person saw in last two weeks).
This library will allow to answer following questions:

* How many 18-24 yo males saw keyword 'iPhone 6S' or 'iPhone 6S Plus' in last two weeks?
* What is the random sample of 10k males who are 18-24 yo and saw 'iPhone 6S' or 'iPhone 6S Plus' in last two weeks?

## Usage

You will need to provide few parameters when initializing the library:

* `:db-uris` -- list of redis instances. Make sure you use the same order of the instances in all cases when you use same redis cluster.
* `:f-index` -- function that extracts stuff you want to index from the object you want to index (see example below). It accepts the object and should return a list of tuples `(prefix, values)`. Try to keep prefix short, especially in cases when there can be lots of values. Examples below.

Following parameters are optional.

* `:f-id->iid` and `:f-iid->id` -- functions that map object ids into internal ids. If you don't provide these, internal implementation will be used. Internal implementation generates short sequential IDs. Should be ok in most cases. You will want to provide your's if your IDs are already short for example.

```clojure
(let [client (db/init {:db-uris [(str "redis://" redis-host ":26269")
                                 (str "redis://" redis-host ":26279")]
                       :f-index (fn [obj]
                                  [[:gender   (:gender obj)]
                                   [:age      (:age obj)]
                                   [:income   (:income obj)]
                                   [:cc       (:cc obj)]
                                   [:f        (:frequent-keywords obj)]
                                   [:u        (:uw obj)]
                                   [:s        (:gsw obj)]
                                   [:d        (:td obj)]
                                   [:race     (:race obj)]
                                   [:z        (:zipcode obj)]])})]
  (db/query client {:gender [:male] :age [:18-24 :25-34]}))
```


## License

Copyright © 2016 Amobee.
