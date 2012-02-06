(defproject test "1.0.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [cascalog "1.8.5"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.204.0"]]
  :aot [test.nested test.tuples]
  :source-path "src")
