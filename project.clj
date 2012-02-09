(defproject test "1.0.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [cascalog "1.8.5"]
                 [cascading.pingles/cascading.cassandra "0.0.1-SNAPSHOT"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.204.0"]]
  :repositories {"conjars" "http://conjars.org/repo"}
  :aot [test.nested test.tuples test.cassandra]
  :source-path "src")
