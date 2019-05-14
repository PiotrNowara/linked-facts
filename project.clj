(defproject linked-facts "0.1.0-SNAPSHOT"
  :description "Linked Facts"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ;[tesser.core "1.0.3"] [clojure-csv/clojure-csv "2.0.1"] [clj-time "0.14.0"] [org.clojure/data.csv "0.1.4"]
                 ]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options" "-Xmx6g"]
  ;:aot [linked-facts.core]
  ;:main linked-facts.core
  )
