(ns test.cassandra
  (:use cascalog.api)
  (:require [clojure.string :as string])
  (:require [cascalog.workflow :as w])
  (:require [cascalog [vars :as v] [ops :as o]])
  (:import [org.pingles.cascading.cassandra WideRowScheme CassandraTap])
  (:import [cascading.tuple Tuple TupleEntry Fields])
  (:import [cascading.tuple Fields])
  (:gen-class))

(def artifact-keys
  [:groupid :artifactid :version :size :sha1 :md5 :path :prefix
   :javadoc?  :source? :signature? :classnames :lastmodified
   :packaging :repository :remoteurl :description :fname :fextension
   :bundle_version :bundle_symname :bundle_exportpkg :bundle_exportsrv
   :bundle_description :bundle_name :bundle_license :bundle_docurl
   :bundle_importpkg])

(def field-names
  ["?groupid" "?artifactid" "?version" "?size" "!sha1" "!md5" "!path" "!prefix"
   "?javadoc" "!source" "?signature" "!classnames" "?lastmodified"
   "!packaging" "!repository" "!remoteurl" "!description" "!fname" "!fextension"
   "!bundle_version" "!bundle_symname" "!bundle_exportpkg" "!bundle_exportsrv"
   "!bundle_description" "!bundle_name" "!bundle_license" "!bundle_docurl"
   "!bundle_importpkg"])

(def column-names
  "take the list of field names, strip ! and ? and that will be our
   cassandra column names"
  (doall
   (map #(string/replace %1 "!" "")
        (map #(string/replace %1 "?" "")
             field-names))))

(defn create-artifact-tuple
  "convert the input map to a vector with the values in the order
   supplied by artifact-keys"
  [input]
  (let [artifact (read-string input)]                      ;; read the input string wich is an s-expression serialized hash
    (into [] (map (partial get artifact) artifact-keys)))) ;; get hash values, but put them in the order specified by artifact-keys

(defn get-package-identity
  "our row key generator"
  [groupid artifactid]
  (format "%s|%s" groupid artifactid))

(defmapcatop format-for-output
  "I couldn't figure out how to create a query whoes output field
   count varied depending on the input. So instead I take a wide row
   and flip it vertically so we do a bunch of inserts one column at a
   time. We also convert all values to strings."
  [& fields]
  (let [row-key (get-package-identity (first fields) (first (rest fields)))       ;; get the row key
        nulls-removed (filter #(last %1) (zipmap column-names (map str fields)))] ;; remove fields which have a null value, convert all remaining values to strings
    (map (partial cons row-key) nulls-removed)))                                  ;; append row key to the front of every [col-name, value] tuple

(defn -main
  [input-path]
  (let [source (hfs-textline input-path)
        schema (WideRowScheme.)                                                      ;; create the wide schema, it takes no parameters
        tap (CassandraTap. "localhost" (Integer/valueOf 9160) "test" "test" schema)] ;; connect to the cluster on localhost, keyspace test, column family test
    (?<- tap
         [?row-key ?col ?val]
         (source ?line)                                                              ;; pull a line in from the input file
         (create-artifact-tuple ?line :>> field-names)                               ;; convert input string to a tuple of named fields
         (format-for-output :<< field-names :> ?row-key ?col ?val))))                ;; convert tuple to a series of [row-key, col-name, value] tuples
