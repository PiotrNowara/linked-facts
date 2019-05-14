(ns linked-facts.core
 (:require [clojure.core.reducers :as r])
)

; 2019-05-14 New version with java.time instead joda-time/clj-time


;;;; UTILS
(defn calculate-temporal-diff [d1 d2 chrono-unit] (.until d2 d1 chrono-unit))

; This is the recommended function because they assume the date is already a date object (does not need to parse it over and over again!).
(defn tdiff
  ([event1 event2] (tdiff event1 event2 :date java.time.temporal.ChronoUnit/DAYS))
  ([event1 event2 chrono-unit] (tdiff event1 event2 :date chrono-unit))
  ([event1 event2 date-key chrono-unit] (calculate-temporal-diff (get event1 date-key) (get event2 date-key) chrono-unit)))

(defn tdiff-str
  ([event1 event2] (tdiff-str event1 event2 :date java.time.temporal.ChronoUnit/DAYS))
  ([event1 event2 chrono-unit] (tdiff-str event1 event2 :date chrono-unit))
  ([event1 event2 date-key chrono-unit] (tdiff (java.time.LocalDate/parse (get event1 date-key)) (java.time.LocalDate/parse (get event2 date-key)) chrono-unit)))



;;;; MAIN FNs


;; NEW functions with separated common parts: lf1 and lf2+

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
     (fn ([]  (hash-map currlist []))
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
        (cond
          (= i 0)
          (recur (into [] (pmap (fn [fact] (lf1 fact (nth filters i) initcoll)) initcoll)) (inc i))
          :else
          (recur
           (into []
             (r/flatten;TODO: try r/mapcat identity like in lfx-gr
              (pmap
               (fn [currfacts]
                 (let [klist (first (keys currfacts))];a b
                   (pmap
                    (fn [last-match];events matched in previous level
                      (lf2+ klist last-match (nth filters i) initcoll))
                    (get currfacts klist))))
               lfcoll)))
           (inc i)))))))

(defn lfx-lazy
  [initcoll & filters]
  (let [filtercount (count filters)]
    (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
      (if (= i filtercount)
        lfcoll
        (cond
          (= i 0)
          (recur (pmap (fn [fact] (lf1 fact (nth filters i) initcoll)) initcoll) (inc i))
          :else
          (recur
           (mapcat identity
            (pmap
             (fn [currfacts]
               (let [klist (first (keys currfacts))];a b
                 (pmap
                  (fn [last-match];events matched in previous level
                    (lf2+ klist last-match (nth filters i) initcoll))
                  (get currfacts klist))))
             lfcoll))
           (inc i)))))))



;; NEGS

(defn- lfneg2+
  [klist last-match lvlfilter initcoll]
  (let [currlist (conj klist last-match)];a b c
    (r/fold
     (r/monoid #(merge-with into %1 %2) (constantly {}))
     (fn ([] (hash-map klist []))
         ([rcoll currf]
           (if (apply lvlfilter (conj currlist currf))
             (assoc rcoll klist (conj (get rcoll klist) currf))
             rcoll)))
     initcoll)))

(defn- lfneg1 [a b negfilter initcoll]
  (let [currlist (vector a)]
    (r/fold
     (r/monoid #(merge-with into %1 %2) (constantly {}))
     (fn ([] (hash-map currlist []))
         ([rcoll c]
            (if (negfilter a b c)
              (assoc rcoll currlist (conj (get rcoll a) b))
              rcoll)))
     initcoll)))

(defn lfx-neg-gr [initcoll & filters]
  (let [filtercount (count filters)]
  (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
    (if (= i filtercount)
      lfcoll
      (let [no-neg? (clojure.test/function? (nth filters i))
            pos-filter (if no-neg? (nth filters i) (first (nth filters i)))
            neg-filter (if no-neg? nil (second (nth filters i)))
            pos-coll (cond
                       (= i 0)
                       (into [] (r/map (fn [fact] (lf1 fact pos-filter initcoll)) initcoll))
                       :else
                       (into []
                         (r/mapcat
                          (fn [currfacts]
                            (let [klist (first (keys currfacts))];a b
                             (r/map
                              (fn [last-match];events matched in previous level
                                (lf2+ klist last-match pos-filter initcoll))
                              (get currfacts klist))))
                         lfcoll)))]
      (if no-neg?
        (recur pos-coll (inc i))
        (recur
         (clojure.set/difference
          (set pos-coll)
          (set
           (if (= i 0)
             (into []
              (r/mapcat
               (fn [currfacts]
                 (let [a (first (keys currfacts))]
                   (r/map
                    (fn [b] (lfneg1 a b neg-filter initcoll))
                    (get currfacts a))))
               pos-coll));needs to operate on current coll
             (into []
              (r/mapcat
               (fn [currfacts] ;(println (keys (mapcat identity currfacts)))
                 (let [klist (first (keys currfacts))];a b
                   (r/map
                    (fn [last-match];events matched in previous level
                      (lfneg2+ klist last-match neg-filter initcoll)
                      (r/fold
                       merge
                       (fn ([] (hash-map klist '()))
                           ([rcoll currf]
                             (if (apply neg-filter (flatten (list klist last-match currf)))
                               (assoc rcoll klist (conj (get rcoll klist) currf))
                               rcoll)))
                       initcoll))
                      (get currfacts klist))))
               pos-coll)))))
         (inc i))))))))

;TODO: lfx-neg-gr. lazy for negs?
(defn lfx-neg-s [initcoll & filters]
  (let [filtercount (count filters)]
  (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
    (if (= i filtercount)
      lfcoll
      (let [no-neg? (clojure.test/function? (nth filters i))
            pos-filter (if no-neg? (nth filters i) (first (nth filters i)))
            neg-filter (if no-neg? nil (second (nth filters i)))
            pos-coll (cond
                       (= i 0)
                       (into [] (pmap (fn [fact] (lf1 fact pos-filter initcoll)) initcoll))
                       :else
                       (into []
                         (r/flatten
                          (pmap
                           (fn [currfacts]
                             (let [klist (first (keys currfacts))];a b
                               (pmap
                                (fn [last-match];events matched in previous level
                                  (lf2+ klist last-match pos-filter initcoll))
                                (get currfacts klist))))
                           lfcoll))))]
      (if no-neg?
        (recur pos-coll (inc i))
        (recur
         (clojure.set/difference
          (set pos-coll)
          (set
           (if (= i 0)
             (into []
              (r/flatten
               (pmap
                (fn [currfacts]
                  (let [a (first (keys currfacts))]
                    (pmap
                     (fn [b] (lfneg1 a b neg-filter initcoll))
                     (get currfacts a))))
                pos-coll)));needs to operate on current coll
             (into []
              (r/flatten
               (pmap
                (fn [currfacts] ;(println (keys (mapcat identity currfacts)))
                  (let [klist (first (keys currfacts))];a b
                    (pmap
                     (fn [last-match];events matched in previous level
                       (lfneg2+ klist last-match neg-filter initcoll)
                       (r/fold
                         merge
                         (fn ([] (hash-map klist '()))
                             ([rcoll currf]
                               (if (apply neg-filter (flatten (list klist last-match currf)))
                                 (assoc rcoll klist (conj (get rcoll klist) currf))
                                 rcoll)))
                         initcoll))
                     (get currfacts klist))))
                pos-coll))))))
         (inc i))))))))


(defn lfx-neg-lazy [initcoll & filters]
  (let [filtercount (count filters)]
  (loop [lfcoll [] i 0];lfcoll  is a collwith accumulated results of each level
    (if (= i filtercount)
      lfcoll
      (let [no-neg? (clojure.test/function? (nth filters i))
            pos-filter (if no-neg? (nth filters i) (first (nth filters i)))
            neg-filter (if no-neg? nil (second (nth filters i)))
            pos-coll (cond
                       (= i 0)
                       (pmap (fn [fact] (lf1 fact pos-filter initcoll)) initcoll)
                       :else
                       (mapcat identity
                         (pmap
                          (fn [currfacts]
                            (let [klist (first (keys currfacts))];a b
                             (pmap
                              (fn [last-match];events matched in previous level
                                (lf2+ klist last-match pos-filter initcoll))
                              (get currfacts klist))))
                         lfcoll)))]
      (if no-neg?
        (recur pos-coll (inc i))
        (recur
         (clojure.set/difference
          (set pos-coll)
          (set
           (if (= i 0)
             (mapcat identity
              (pmap
               (fn [currfacts]
                 (let [a (first (keys currfacts))]
                   (pmap
                    (fn [b] (lfneg1 a b neg-filter initcoll))
                    (get currfacts a))))
               pos-coll));needs to operate on current coll
             (mapcat identity
              (pmap
               (fn [currfacts] ;(println (keys (mapcat identity currfacts)))
                 (let [klist (first (keys currfacts))];a b
                   (pmap
                    (fn [last-match];events matched in previous level
                      (lfneg2+ klist last-match neg-filter initcoll)
                      (r/fold
                       merge
                       (fn ([] (hash-map klist '()))
                           ([rcoll currf]
                             (if (apply neg-filter (flatten (list klist last-match currf)))
                               (assoc rcoll klist (conj (get rcoll klist) currf))
                               rcoll)))
                       initcoll))
                      (get currfacts klist))))
               pos-coll)))))
         (inc i))))))))


;;; UTILS

(defn lf-non-empty [res] (filter not-empty (map #(filter not-empty %) res)))
(defn lf-group-by-hits-count [res] (filter #(< 0 (first %)) (group-by #(count (mapcat identity (vals %))) res))); groups results by counts without empty hits
(defn lf-summary-by-hits [res] (map #(hash-map (first %) (count (second %))) (group-by #(count (mapcat identity (vals %))) res)))
