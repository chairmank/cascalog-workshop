(defproject cascalog-workshop "0.1.0-SNAPSHOT"
  :description "An introduction to Cascalog"
  :url "https://github.com/chairmanK/cascalog-workshop.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog "1.10.1"]]
  :plugins [[lein-marginalia "0.7.1"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                                  [lein-midje "3.0.1"]
                                  [cascalog/midje-cascalog "1.10.1"]]}}
  :min-lein-version "2.0.0")
