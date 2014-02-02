(ns trending-terms.core
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (reducers :as pr)]
            [parkour.io (seqf :as seqf) (avro :as mra) (sample :as sample)])
  (:import [org.apache.hadoop.mapreduce Mapper]))

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
        year (Long/parseLong year)
        n (Long/parseLong n)
        decade (-> year (quot 10) (* 10))]
    [(str/lower-case gram) decade n]))

(defn select-record?
  [[gram year n]]
  (and (<= 1890 year)
       (re-matches #"^[A-Za-z+'-]+$" gram)))

(defn normalized-m
  {::mr/source-as :vals, ::mr/sink-as :keys}
  [records]
  (->> records
       (r/map parse-record)
       (r/filter select-record?)))

(defn normalized-j
  [conf workdir ngrams]
  (let [outpath (fs/path workdir "normalized")
        schema (avro/tuple-schema ['string 'long 'long])]
    (-> (pg/input ngrams)
        (pg/map #'normalized-m)
        (pg/output (mra/dsink [schema] outpath))
        (pg/execute conf "normalized")
        first)))

(defn sum-r
  {::mr/source-as :keyvalgroups, ::mr/sink-as :keyvals}
  [input] (r/map (fn [[k vs]] [k (reduce + 0 vs)]) input))

(defn decade-totals-m
  {::mr/source-as :keys, ::mr/sink-as :keyvals}
  [input]
  (->> input
       (r/map (fn [[gram year n]]
                [year n]))))

(defn gram-count-m
  {::mr/source-as :keys, ::mr/sink-as :keys}
  [input]
  (->> input
       (r/map (fn [[gram year n]] gram))
       (pr/distinct)))

(defn gram-count-r
  {::mr/source-as :keygroups, ::mr/sink-as :keys}
  [input] [(reduce (fn [n _] (inc n)) 0 input)])

(defn decade-totals-j
  [conf workdir normalized]
  (let [decades-path (fs/path workdir "decade-totals")
        grams-path (fs/path workdir "gram-count")
        [decades grams]
        , (-> [(-> (pg/input normalized)
                   (pg/map #'decade-totals-m)
                   (pg/partition (mra/shuffle 'long 'long))
                   (pg/combine #'sum-r)
                   (pg/reduce #'sum-r)
                   (pg/output (mra/dsink ['long 'long] decades-path)))
               (-> (pg/input normalized)
                   (pg/map #'gram-count-m)
                   (pg/partition (mra/shuffle 'string))
                   (pg/reduce #'gram-count-r)
                   (pg/output (mra/dsink ['long] grams-path)))]
              (pg/execute conf "totals"))
        gramsc (reduce + 0 (r/map first grams))]
    (->> decades
         (r/map (fn [[decade n]]
                  [decade (double (+ gramsc n))]))
         (into {}))))

(defn trending-m
  {::mr/source-as :keys, ::mr/sink-as :keyvals}
  [input]
  (->> input
       (r/map (fn [[gram decade n]]
                [[gram decade] n]))
       (pr/reduce-by first (pr/mjuxt pr/arg1 +) [nil 0])))

(defn first-p
  [k v n] (-> k first hash (mod n)))

(defn trending-r
  {::mr/source-as :keyvalgroups, ::mr/sink-as :keyvals}
  [decades input]
  (->> (sum-r input)
       (r/map (fn [[[gram decade] n]] [gram [decade n]]))
       (pr/reduce-by first (pr/mjuxt pr/arg1 conj) [nil {}])
       (r/mapcat (fn [[gram counts]]
                   (let [ratios (reduce-kv (fn [m dy nd]
                                             (let [ng (inc (counts dy 0))]
                                               (assoc m dy (/ ng nd))))
                                           {} decades)]
                     (->> (seq ratios)
                          (r/map (fn [[dy r]]
                                   (let [r' (ratios (- dy 10))]
                                     (if (and r' (< 0.000001 r))
                                       [[dy (/ r r')] gram]))))
                          (r/remove nil?)))))))

(defn trending-topn-r
  {::mr/source-as :keyvalgroups, ::mr/sink-as :keyvals}
  [n input]
  (r/map (fn [[[decade] grams]]
           [decade (into [] (r/take n grams))])
         input))

(defn trending-j
  [conf workdir normalized decades topn]
  (let [out0-path (fs/path workdir "trending/0")
        out1-path (fs/path workdir "trending/1")
        gram-decade (avro/tuple-schema ['string 'long])
        decade-ratio (avro/parse-schema
                      {:type :record, :name 'decade-ratio,
                       :abracad.reader 'vector,
                       :fields [{:name 'decade, :type 'long}
                                {:name 'ratio, :type 'double,
                                 :order 'descending}]})
        decade-group (avro/grouping-schema 1 decade-ratio)
        grams-array {:type 'array, :items 'string}
        [trending]
        , (-> (pg/input normalized)
              (pg/map #'trending-m)
              (pg/partition (mra/shuffle gram-decade 'long) #'first-p)
              (pg/reduce #'trending-r decades)
              (pg/output (mra/dsink [decade-ratio 'string] out0-path))
              (pg/map Mapper)
              (pg/partition (mra/shuffle decade-ratio 'string decade-group)
                            #'first-p)
              (pg/reduce #'trending-topn-r topn)
              (pg/output (mra/dsink ['long grams-array] out1-path))
              (pg/execute conf "trending"))]
    (into {} trending)))

(defn trending-terms
  [conf workdir ngrams topn]
  (let [normalized (normalized-j conf workdir ngrams)
        decades (decade-totals-j conf workdir normalized)]
    (trending-j conf workdir normalized decades topn)))
