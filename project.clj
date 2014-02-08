(defproject trending-terms "0.1.0-SNAPSHOT"
  :description "Decade-wise trending terms in the Google Books n-grams corpus"
  :url "http://github.com/llasram/trending-terms"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.damballa/parkour "0.5.4-SNAPSHOT"]
                 [org.apache.avro/avro "1.7.5"]
                 [org.apache.avro/avro-mapred "1.7.5"
                  :classifier "hadoop2"]
                 [transduce/transduce "0.1.1"]]
  :exclusions [org.apache.hadoop/hadoop-core
               org.apache.hadoop/hadoop-common
               org.apache.hadoop/hadoop-hdfs
               org.slf4j/slf4j-api org.slf4j/slf4j-log4j12 log4j
               org.apache.avro/avro
               org.apache.avro/avro-mapred
               org.apache.avro/avro-ipc]
  :profiles {:provided
             {:dependencies [[org.apache.hadoop/hadoop-client "2.2.0"]
                             [org.apache.hadoop/hadoop-common "2.2.0"]
                             [org.slf4j/slf4j-api "1.6.1"]
                             [org.slf4j/slf4j-log4j12 "1.6.1"]
                             [log4j "1.2.17"]]}
             :test {:resource-paths ["test-resources"]}
             :aot {:aot :all, :compile-path "%s/aot/class"}
             :uberjar [:aot]
             :jobjar [:aot]})
