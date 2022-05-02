(ns linked-facts.core
 (:require [clojure.core.reducers :as r]))

;;;04.2022 - v. 0.2 - bugfixes, tdiff cache, cleanup

;;;; UTILS
(defn calculate-temporal-diff [d1 d2 chrono-unit] (.until d2 d1 chrono-unit))


(defn tdiff
  ([event1 event2] (tdiff event1 event2 :date java.time.temporal.ChronoUnit/DAYS))
  ([event1 event2 chrono-unit] (tdiff event1 event2 :date chrono-unit))
  ([event1 event2 date-key chrono-unit] (calculate-temporal-diff (get event1 date-key) (get event2 date-key) chrono-unit)))

(def date-diffs (atom {}))
(defn tdiff-cached
  ([event1 event2] (tdiff-cached event1 event2 :date java.time.temporal.ChronoUnit/DAYS))
  ([event1 event2 chrono-unit] (tdiff-cached event1 event2 :date chrono-unit))
  ([event1 event2 date-key chrono-unit] (let [ev1-key (get event1 date-key)
                                              ev2-key (get event2 date-key)
                                              diff-key (str ev1-key ev2-key)
                                              diff-res (get @date-diffs diff-key)]
                                          (if (nil? diff-res)
                                            (let [new-diff (calculate-temporal-diff ev1-key ev2-key chrono-unit)]
                                              (swap! date-diffs assoc diff-key new-diff)
                                              new-diff)
                                            diff-res))))

(defn tdiff-str
  ([event1 event2] (tdiff-str event1 event2 :date java.time.temporal.ChronoUnit/DAYS))
  ([event1 event2 chrono-unit] (tdiff-str event1 event2 :date chrono-unit))
  ([event1 event2 date-key chrono-unit] (tdiff (java.time.LocalDate/parse (get event1 date-key)) (java.time.LocalDate/parse (get event2 date-key)) chrono-unit)))


;;;; MAIN FNs

(defn- lf1
  [a lvlfilter initcoll]
  (let [currlist (vector a)]
    (r/fold
     (r/monoid #(merge-with into %1 %2) (constantly {}))
     (fn ([] (hash-map currlist []))
       ([rcoll b]
        (if (lvlfilter a b)
          (assoc rcoll currlist (conj (get rcoll a) b))
          rcoll)))
     initcoll)))

(defn- lf2+
  [klist last-match lvlfilter initcoll]
  (let [currlist (conj klist last-match)];a b c
    (r/fold
     (r/monoid #(merge-with into %1 %2) (constantly {}))
     (fn ([] (hash-map currlist []))
       ([rcoll currf]
        (if (apply lvlfilter (conj currlist currf))
          (assoc rcoll currlist (conj (get rcoll currlist) currf))
          rcoll)))
     initcoll)))


;; Fastest with grouping (pmap on grouping), very slow without grouping (no pmap).
(defn lfx-gr
  [initcoll & filters]
  (let [filtercount (count filters)]
    (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
      (if (= i filtercount)
        lfcoll
        (cond
          (= i 0)
          (recur (into [] (r/map (fn [fact] (lf1 fact (nth filters i) initcoll)) initcoll)) (inc i))
          :else
          (recur
           (into []
            (r/mapcat
             (fn [currfacts]
               (let [klist (first (keys currfacts))];a b
                 (r/map
                  (fn [last-match];events matched in previous level
                    (lf2+ klist last-match (nth filters i) initcoll))
                  (get currfacts klist))))
             lfcoll))
           (inc i)))))))


;; It's the fastes with big chunks of data without frouping.
(defn lfx-s
  [initcoll & filters]
  (let [filtercount (count filters)]
    (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
      (if (= i filtercount)
        lfcoll
        (if (= i 0)
          (recur (into [] (r/filter not-empty (pmap (fn [fact] (lf1 fact (nth filters i) initcoll)) initcoll))) (inc i))
          (recur
           (into []
                 (r/filter
                  not-empty
                  (r/mapcat (fn [lvl-data-elem]
                              (r/mapcat
                               (fn [klist]
                                 (pmap
                                  (fn [last-match];events matched in previous level
                                    (lf2+ klist last-match (nth filters i) initcoll))
                                  (get lvl-data-elem klist)))
                               (keys lvl-data-elem)))
                            lfcoll)))
           (inc i)))))))

;; Fastest with grouping (pmap on grouping), very slow without grouping (no pmap).
(defn lfx-gr
  [initcoll & filters]
  (let [filtercount (count filters)]
    (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
      (if (= i filtercount)
        lfcoll
        (if (= i 0)
          (recur (into [] (r/filter not-empty (r/map (fn [fact] (lf1 fact (nth filters i) initcoll)) initcoll)) (inc i)) (inc i))
          (recur
           (into []
                 (r/filter
                  not-empty
                  (r/mapcat (fn [lvl-data-elem]
                              (r/mapcat
                               (fn [klist]
                                 (pmap
                                  (fn [last-match];events matched in previous level
                                    (lf2+ klist last-match (nth filters i) initcoll))
                                  (get lvl-data-elem klist)))
                               (keys lvl-data-elem)))
                            lfcoll)))
           (inc i)))))))

(defn lfx-lazy
  [initcoll & filters]
  (let [filtercount (count filters)]
    (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
      (if (= i filtercount)
        lfcoll
        (if (= i 0)
          (recur (pmap (fn [fact] (lf1 fact (nth filters i) initcoll)) initcoll) (inc i))
          (recur
           (into []
                 (r/filter
                  not-empty
                  (r/mapcat (fn [lvl-data-elem]
                              (r/mapcat
                               (fn [klist]
                                 (pmap
                                  (fn [last-match];events matched in previous level
                                    (lf2+ klist last-match (nth filters i) initcoll))
                                  (get lvl-data-elem klist)))
                               (keys lvl-data-elem)))
                            lfcoll)))
           (inc i)))))))



;;; UTILS

(defn lf-non-empty [res] (filter not-empty (map #(filter not-empty %) res)))
(defn lf-group-by-hits-count [res] (filter #(< 0 (first %)) (group-by #(count (mapcat identity (vals %))) res))); groups results by counts without empty hits
(defn lf-summary-by-hits [res] (map #(hash-map (first %) (count (second %))) (group-by #(count (mapcat identity (vals %))) res)))

