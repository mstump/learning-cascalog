;; Given an input file, which has one s-expression serialized hash per line reduce the hashes into one giant nested hash
;; The outer hash is indexed by groupid, each sub hash is indexed by artifactid, whose values is yet another hash indexed by version.
;; Not particularly useful, but it's something I was curious about.

(ns test.nested
  (:use cascalog.api)
  (:require [cascalog [vars :as v]])
  (:gen-class))

(defn textline-parsed [dir]
  "parse input file, it's one hash serialized as an s-expression per line"
  (let [outargs (v/gen-nullable-vars 1)
        source (hfs-textline dir)]
    (<- outargs (source ?line)
        (read-string ?line :>> outargs)
        (:distinct false))))

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
  ([] {})
  ([state val] (append-state state val))
  ([state] [state]))

(defn query
  [output-tap input-path]
  (let [artifacts (textline-parsed input-path)]
    (?<- output-tap
         [?group]
         (artifacts ?a)
         (group-by-group-id ?a :> ?group))))

(defn -main [input-path output-dir]
  (query (hfs-textline output-dir) input-path))
