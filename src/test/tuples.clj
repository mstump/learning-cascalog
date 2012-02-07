(ns test.tuples
  (:use cascalog.api)
  (:require [cascalog [vars :as v] [ops :as o]])
  (:gen-class))

(def artifact-keys
  [:groupid :artifactid :version :size :sha1 :md5 :path :prefix :javadoc?
   :source? :signature? :classnames :lastmodified :packaging :repository
   :remoteurl :description :fname :fextension :bundle_version
   :bundle_symname :bundle_exportpkg :bundle_exportsrv
   :bundle_description :bundle_name :bundle_license :bundle_docurl
   :bundle_importpkg])

(defn create-artifact-tuple
  "convert the input map to a vector with the values in the order
   supplied by artifact-keys"
  [input]
  (let [artifact (read-string input)]
    (into [] (map (partial get artifact) artifact-keys))))

(defn textline-parsed
  "take a line from the input tap which is a hash serialized as an
   s-expression, and turn it into a tuple with named fields"
  [dir]
  (let [source (hfs-textline dir)]
    (<- [?groupid ?artifactid ?version ?size !sha1 !md5 !path
         !prefix ?javadoc? !source? ?signature? !classnames
         ?lastmodified !packaging !repository !remoteurl
         !description !fname !fextension !bundle_version
         !bundle_symname !bundle_exportpkg !bundle_exportsrv
         !bundle_description !bundle_name !bundle_license
         !bundle_docurl !bundle_importpkg]
        (source ?line)
        (create-artifact-tuple ?line :> ?groupid ?artifactid ?version ?size !sha1 !md5 !path
                               !prefix ?javadoc? !source? ?signature? !classnames
                               ?lastmodified !packaging !repository !remoteurl
                               !description !fname !fextension !bundle_version
                               !bundle_symname !bundle_exportpkg !bundle_exportsrv
                               !bundle_description !bundle_name !bundle_license
                               !bundle_docurl !bundle_importpkg))))

(defn query-uniq-group-ids
  "return distinct occurances of ?groupid ?version and !fextension"
  [output-tap input-path]
  (?<- output-tap
       [?groupid ?version !fextension]
       ((select-fields (textline-parsed input-path)
                       ["?groupid" "?version" "!fextension"])
        ?groupid ?version !fextension )
       (:distinct true)))

(defn max-lastchanged
  "return max lastchanged for each group ?groupid ?version and !fextension"
  [input]
  (o/first-n input
             1
             :sort "?lastmodified"
             :reverse true))

(defn get-package-identity
  [groupid artifactid]
  (str groupid "/" artifactid))

(defn get-identity-count
  [input-path]
  (?<- (stdout)
       [?identity ?count]
       ((select-fields (textline-parsed input-path) ["?groupid" "?artifactid"]) ?groupid ?artifactid)
       (get-package-identity ?groupid ?artifactid :> ?identity)
       (o/count ?count)))

(defn get-row-count
  [input-path]
  (?<- (stdout)
       [?count]
       ((select-fields (textline-parsed input-path) ["?groupid"]) ?groupid)
       (o/count ?count)))


;; (?<- (stdout) (textline-parsed input-path) input-path)              ;; print all rows
;; (query-uniq-group-ids (stdout) input-path))) ;; print rows which have a unique identity
