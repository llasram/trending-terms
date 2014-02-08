(ns trending-terms.core-test
  (:require [clojure.test :refer :all]
            [trending-terms.core :as tt]
            [parkour (fs :as fs) (conf :as conf)]
            [parkour.io (dsink :as dsink) (seqf :as seqf)]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io Text LongWritable]))

(def n1grams-records
  [ ;; Randomly sampled then tweaked input
   [7881580725 "edge\t1994\t2\t2\t2"]
   [6283203307 "legislature\t1887\t1\t1\t1"]
   [6283203307 "legislature\t1888\t1\t1\t1"]
   [6283203307 "legislature\t1889\t2\t1\t1"]
   [6283189334 "legal\t1952\t9\t9\t8"]
   [6006878252 "Asmonean\t1902\t1\t1\t1"]
   [6006892169 "Astoria\t1979\t6\t6\t6"]
   [10293576694 "\"evils\t1970\t1\t1\t1"]
   [5178688306 "\"propositions\t1943\t16\t16\t4"]
   [2743033014 "bodies\t1973\t1\t1\t1"]
   [2743022621 "\"boards\t2002\t10\t10\t10"]
   [3003483252 "inflated\t1858\t1\t1\t1"]
   [3897727086 "\"\t1884\t5\t5\t5"]
   [6006880298 "Assembly\t1964\t2\t2\t2"]
   [7881572183 "economy\t1944\t2\t2\t2"]
   [7881581342 "edged\t1971\t1\t1\t1"]
   [5486744827 "placed\t1823\t1\t1\t1"]
   [5178705674 "prostate\t1999\t1\t1\t1"]
   [5178705674 "prostate\t2004\t35\t35\t30"]
   [1737556075 "2000\t1918\t13\t13\t10"]
   [10809388147 "showed\t1856\t1\t1\t1"]
   [3003495538 "influence\t1964\t1\t1\t1"]
   [3897718037 "\"\t1993\t36\t32\t26"]
   [3897718692 "\"\t2003\t3\t3\t3"]
   [7881565433 "economically\t1940\t20\t20\t16"]
   [2743039772 "\"body \t1858\t2\t2\t2"]
   [10293566245 "evident\t1920\t8\t8\t8"]
   [7881586748 "editing\t1922\t12\t12\t12"]])

(deftest test-basic
  (th/with-config
    (let [workdir (doto "tmp/work/basic" fs/path-delete)
          inpath (fs/path workdir "ngrams")
          ngrams (dsink/with-dseq (seqf/dsink [LongWritable Text] inpath)
                   n1grams-records)
          trending (tt/trending-terms (th/config) workdir ngrams 1)]
      (is (= {1950 ["legal"],
              1960 ["assembly"],
              1970 ["astoria"],
              2000 ["prostate"]}
             trending)))))
