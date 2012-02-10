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
  (doall
   (map #(string/replace %1 "!" "")
        (map #(string/replace %1 "?" "")
             field-names))))

(defn create-artifact-tuple
  "convert the input map to a vector with the values in the order
   supplied by artifact-keys"
  [input]
  (let [artifact (read-string input)]
    (into [] (map (partial get artifact) artifact-keys))))

(defn get-package-identity
  [groupid artifactid]
  (format "%s|%s" groupid artifactid))

(defmapcatop format-for-output
  [& fields]
  (let [row-key (get-package-identity (first fields) (first (rest fields)))
        nulls-removed (filter #(last %1) (zipmap column-names (map str fields)))]
    (map (partial cons row-key) nulls-removed)))

(defn -main
  [input-path]
  (let [source (hfs-textline input-path)
        schema (WideRowScheme.)
        tap (CassandraTap. "localhost" (Integer/valueOf 9160) "test" "test" schema)]
    (?<- tap
         [?row-key ?col ?val]
         (source ?line)
         (create-artifact-tuple ?line :>> field-names)
         (format-for-output :<< field-names :> ?row-key ?col ?val))))



;;         )))
;;         (format-for-output :<< field-names :>> output-fields))))
;;
;; (let []
;; (max-lastchanged (stdout) input-path))
;; (?<- (stdout) (textline-parsed input-path) input-path)              ;; print all rows
;; (query-uniq-group-ids (stdout) input-path))) ;; print rows which have a unique identity
;; schema (CassandraScheme. (w/fields "?name") (w/fields-array ["?name"]))