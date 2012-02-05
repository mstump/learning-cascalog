;; Given an input file, which has one s-expression serialized hash per
;; line reduce the hashes into one giant nested hash The outer hash is
;; indexed by groupid, each sub hash is indexed by artifactid, whose
;; values is yet another hash indexed by version.  Not particularly
;; useful, but it's something I was curious about.

(ns test.nested
  (:use cascalog.api)
  (:require [cascalog [vars :as v]])
  (:gen-class))

(defn textline-parsed [dir]
  "This function returns a query which parses an input file. The input
   format is one hash serialized as an s-expression per line"
  ;; This is pulled almost directly from http://nathanmarz.com/blog/news-feed-in-38-lines-of-code-using-cascalog.html
  ;; It was originally a dynamic query which explains the presense of gen-nullable-vars, which generates variables names from UUIDs
  ;; It can be converted by taking outargs out of the let binding form, rename outargs to ?outargs and use :> instead of :>>
  ;;
  ;; The static query version:
  ;;
  ;; (let [source (hfs-textline dir)]
  ;;   (<- [?outargs]
  ;;       (source ?line)
  ;;       (read-string ?line :> ?outargs)
  ;;       (:distinct false))))
  ;;
  (let [outargs (v/gen-nullable-vars 1) ;; Generate a dynamic list of vars, see above.
        source (hfs-textline dir)]      ;; Create a tap which pulls lines from a text file in HFS.
    (<- outargs
        (source ?line)                  ;; Pull one line from the source tap, and place it in ?line.
        (read-string ?line :>> outargs) ;; Convert the s-expression to a hash, put the output in the output variable.
        (:distinct false))))            ;; I assume allow duplicates, but haven't played with it yet.

(defn append-state
  "group maven artifacts as a series of nested hashes
  { groupid
     { artifactid
       { version {artifact} }}}
  "
  [state val]
  (let [groupid (:groupid val)
        artifactid (:artifactid val)
        version (:version val)
        group (get state groupid)
        artifact (get group artifactid)]

    (assoc state groupid
           (assoc group artifactid
                  (assoc artifact version val)))))

(defaggregateop group-by-group-id
  ([] {})                                ;; Initialization
  ([state val] (append-state state val)) ;; Append the next element to be reduced to the accumulated state
  ([state] [state]))                     ;; Return after reducing

(defn query
  [output-tap input-path]
  (let [artifacts (textline-parsed input-path)] ;; Define a var to the textline-parsed query.
    (?<- output-tap                             ;; Query whoes output is the specifed output HFS file
         [?group]                               ;; The output variable
         (artifacts ?a)                         ;; So they call this a 'predicate' esentialy, where to get data and what to do with it.
                                                ;; We are getting data from another query 'artifacts' which is really a call
                                                ;; to textline-parsed which is defined above.
         (group-by-group-id ?a :> ?group))))    ;; reduce the input variable ?a and output (:>) to the output variable ?group

(defn -main [input-path output-dir]
  (query (hfs-textline output-dir) input-path))
