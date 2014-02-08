(ns trending-terms.core
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [transduce.reducers :as tr]
            [abracad.avro :as avro]
            [abracad.avro.edn :as aedn]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (reducers :as pr)]
            [parkour.io (seqf :as seqf) (avro :as mra) (dux :as dux)
             ,          (sample :as sample)])
  (:import [org.apache.hadoop.mapreduce Mapper]))

(set! *warn-on-reflection* true)

(def ngram-base-url
  "Base URL for Google Books n-gram dataset."
  "s3://datasets.elasticmapreduce/ngrams/books/20090715")

(defn ngram-url
  "URL for Google Books `n`-gram dataset."
  [n] (fs/uri ngram-base-url "eng-us-all" (str n "gram") "data"))

(defn ngram-dseq
  "Distributed sequence for Google Books `n`-grams."
  [n] (seqf/dseq (ngram-url n)))

(defn parse-record
  "Parse text-line 1-gram record `rec`."
  [rec]
  (let [[gram year n] (str/split rec #"\t")
        gram (str/lower-case gram)
        year (Long/parseLong year)
        n (Long/parseLong n)
        decade (-> year (quot 10) (* 10))]
    [[gram decade] n]))

(defn select-record?
  [[[gram year]]]
  (and (<= 1890 year)
       (re-matches #"^[a-z+'-]+$" gram)))

(defn normalized-m
  {::mr/source-as :vals, ::mr/sink-as :keyvals}
  [records]
  (->> records
       (r/map parse-record)
       (r/filter select-record?)
       (pr/reduce-by first (pr/mjuxt pr/arg1 +) [nil 0])))

(defn nth0-p
  [k v n] (-> k (nth 0) hash (mod n)))

(defn normalized-r
  {::mr/source-as :keyvalgroups,
   ::mr/sink-as (dux/named-keyvals :totals)}
  [input]
  (let [nwords (.getCounter mr/*context* "normalized" "nwords")
        fnil+ (fnil + 0)]
    (->> input
         (r/map (fn [[[gram decade] ns]]
                  [gram [decade (reduce + 0 ns)]]))
         (pr/reduce-by first (pr/mjuxt pr/arg1 conj) [nil {}])
         (reduce (fn [totals [gram counts]]
                   (dux/write mr/*context* :counts gram (seq counts))
                   (merge-with + totals counts))
                 {})
         (seq))))

(defn trending-m
  {::mr/source-as :keyvals, ::mr/sink-as :keyvals}
  [totals input]
  (r/mapcat (fn [[gram counts]]
              (let [counts (into {} counts)
                    ratios (reduce-kv (fn [m dy nd]
                                        (let [ng (inc (counts dy 0))]
                                          (assoc m dy (/ ng nd))))
                                      {} totals)]
                (->> (seq ratios)
                     (r/map (fn [[dy r]]
                              (let [r' (ratios (- dy 10))]
                                (if (and r' (< 0.000001 r))
                                  [[dy (- (/ r r'))] gram]))))
                     (r/remove nil?))))
            input))

(defn trending-r
  {::mr/source-as :keyvalgroups, ::mr/sink-as :keyvals}
  [n input]
  (r/map (fn [[[decade] grams]]
           [decade (into [] (r/take n grams))])
         input))

(defn trending-terms
  [conf workdir ngrams topn]
  (let [counts-path (fs/path workdir "counts")
        totals-path (fs/path workdir "totals")
        pkey-as (avro/tuple-schema ['string 'long])
        counts-as {:type 'array, :items (avro/tuple-schema ['long 'long])}
        [counts totals]
        , (-> (pg/input ngrams)
              (pg/map #'normalized-m)
              (pg/partition (mra/shuffle pkey-as 'long) #'nth0-p)
              (pg/reduce #'normalized-r)
              (pg/output :counts (mra/dsink ['string counts-as] counts-path)
                         :totals (mra/dsink ['long 'long] totals-path))
              (pg/execute conf "normalized"))
        gramsc (-> (mr/counters-map totals)
                   (get-in ["normalized" "nwords"])
                   double)
        fnil+ (fnil + gramsc)
        totals (reduce (fn [m [d n]]
                         (update-in m [d] fnil+ n))
                       {} totals)
        ratio-as (avro/tuple-schema ['long 'double])
        ratio+g-as (avro/grouping-schema 1 ratio-as)
        grams-array {:type 'array, :items 'string}
        trending-path (fs/path workdir "trending")
        [trending]
        , (-> (pg/input counts)
              (pg/map #'trending-m totals)
              (pg/partition (mra/shuffle ratio-as 'string ratio+g-as)
                            #'nth0-p)
              (pg/reduce #'trending-r topn)
              (pg/output (mra/dsink ['long grams-array] trending-path))
              (pg/execute conf "trending"))]
    (into {} trending)))
