(ns test.tuples
  (:use cascalog.api)
  (:require [cascalog [vars :as v]])
  (:gen-class))

(def artifact-keys
  [:groupid :artifactid :version :size :sha1 :md5 :path :prefix :javadoc?
   :source? :signature? :classnames :lastmodified :packaging :repository
   :remoteurl :description :fname :fextension :bundle_version
   :bundle_symname :bundle_exportpkg :bundle_exportsrv
   :bundle_description :bundle_name :bundle_license :bundle_docurl
   :bundle_importpkg])

(def artifact-fields
  ["?groupid" "?artifactid" "?version" "?size" "!sha1" "!md5" "!path"
   "!prefix" "?javadoc?" "!source?" "?signature?" "!classnames"
   "?lastmodified" "!packaging" "!repository" "!remoteurl"
   "!description" "!fname" "!fextension" "!bundle_version"
   "!bundle_symname" "!bundle_exportpkg" "!bundle_exportsrv"
   "!bundle_description" "!bundle_name" "!bundle_license"
   "!bundle_docurl" "!bundle_importpkg"])

(defn create-artifact-tuple
  [input]
  (let [artifact (read-string input)]
    (into [] (map (partial get artifact) artifact-keys))))

(defn textline-parsed [dir]
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
;        (name-vars ?line artifact-fields))))

(defn query
  [output-tap input-path]
  (?<- output-tap
       [?groupid ?version !fextension ?lastmodified]
       ((select-fields (textline-parsed input-path)
                       ["?groupid" "?version" "!fextension" "?lastmodified"])
        ?groupid ?version !fextension ?lastmodified)))

(defn -main [input-path]
  (query (stdout) input-path))
