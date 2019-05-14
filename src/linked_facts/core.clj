(ns linked-facts.core (:gen-class)
 (:require [clojure.core.reducers :as r])
)

; 2019-05-14 New version with java.time instead joda-time/clj-time


;;;; UTILS
(defn calculate-temporal-diff [d1 d2 chrono-unit] (.until d2 d1 chrono-unit))

(defn tdiff-str
  ([event1 event2] (tdiff-str event1 event2 :date t/in-days))
  ([event1 event2 chrono-unit] (tdiff-str event1 event2 :date chrono-unit))
  ([event1 event2 date-key chrono-unit] (tdiff (java.time.LocalDate/parse (get event1 date-key)) (java.time.LocalDate/parse (get event2 date-key)) chrono-unit)))

; This are the recommended functions because they assume the date is already a date object (does not need to parse it over and over again!).
(defn tdiff
  ([event1 event2] (tdiff event1 event2 :date java.time.temporal.ChronoUnit/DAYS))
  ([event1 event2 chrono-unit] (tdiff event1 event2 :date chrono-unit))
  ([event1 event2 date-key chrono-unit] (calculate-temporal-diff (get event1 date-key) (get event2 date-key) chrono-unit)))


;;;; MAIN FNs


;; NEW functions with separated common parts: lf1 and lf2+
;; started to format code according to Clojure guideliness
;; add docstrings

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

;; TESTING

(defrecord Fact [id cust-id date value]);records should have better performance than ordinary maps
(def src-coll (for [i (range 1 10001)] (linked_facts.core.Fact. i (+ 1 (rand-int 10)) (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) (rand-int 3000))))
(def coll1000 (for [i (range 1000)] (linked_facts.core.Fact. i (+ 1 (rand-int 10)) (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) (rand-int 3000))))


(clojure.pprint/pprint (take 1
  (lfx-lazy
    src-coll
    #(and (= (:cust-id %1) (:cust-id %2)) (> (:value %1) (:value %2)) (> (:value %1) 400) (= 21 (tdiff %1 %2)))
    #(and (= (:cust-id %2) (:cust-id %3)) (> (:value %3) (:value %2)) (= 1 (tdiff %2 %3)))
    #(and (= (:cust-id %3) (:cust-id %4)) (> (:value %4) 200) (= 1 (tdiff %4 %3))))))


(def t1b
  (lfx-s
    src-coll
    #(and (= (:cust-id %1) (:cust-id %2)) (> (:value %1) (:value %2)) (< (:value %1) 400) (= 21 (tdiff %1 %2)))
    #(and (= (:cust-id %2) (:cust-id %3)) (> (:value %3) (:value %2)) (= 1 (tdiff %2 %3)))
    #(and (= (:cust-id %3) (:cust-id %4)) (< (:value %4) 200) (= 1 (tdiff %4 %3)))))

(def t1 (doall
 (pmap
  (fn [cust-data]
    (lfx-gr cust-data
     #(and (> (:value %1) (:value %2)) (= 7 (tdiff %1 %2)))
     #(and (> (:value %2) (:value %3)) (= 7 (tdiff %2 %3)))
     #(and (> (:value %4) 2000) (= 29 (tdiff %4 %3)))
     #(and (> (:value %5) 2900) (= 10 (tdiff %4 %5)))))
  (vals (group-by :cust-id src-coll)))))

(clojure.pprint/pprint (take 1 (mapcat identity t1)))
(def res (filter not-empty (map #(filter not-empty %) t1)))
(remove nil? (map #(get-in (-> % first keys flatten first) [:cust-id]) res)); matched  cust-id values. collection needs to be filtered out of nulls
(filter #(< 0 (first %)) (group-by #(count (mapcat identity (vals %))) (mapcat identity t1))); groups results by counts without empty hits
(map #(hash-map (first %) (count (second %))) (group-by #(count (mapcat identity (vals %))) (mapcat identity t1)))

(-> t1 first flatten)
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


;;;; TESTING
(java.time.LocalDate/of 2018 2 3 )
(defrecord Fact [id cust-id date value]);records should have better performance than ordinary maps
(def coll-date (for [i (range 100000)] (linked_facts.core.Fact. i (+ 1 (rand-int 10)) (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) (rand-int 3000))))
(def coll1000 (for [i (range 1000)] (linked_facts.core.Fact. i (+ 1 (rand-int 10)) (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) (rand-int 3000))))
(def hcoll (for [i (range 100000)] (hash-map :id i :cust-id (+ 1 (rand-int 1000)) :date (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) :value (rand-int 3000))))
(def vec-date (into [] (for [i (range 100000)] (linked_facts.core.Fact. i (+ 1 (rand-int 10)) (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) (rand-int 3000)))))

(time
  ;(into []
(def t ;doall
  (;lfred3rniu
  lfx-lazy
  ;  lfx-s
    coll1000
        #(and
           (= (:cust-id %1) (:cust-id %2))
                        (> (:value %1) (:value %2))
                        (= 21 (tdiff %1 %2)))
     #(and       (= (:cust-id %2) (:cust-id %3)) (> (:value %3) (:value %2)) (= 1 (tdiff %2 %3)))
           #(and    (= (:cust-id %3) (:cust-id %4)) (> (:value %4) 200) (< 200 (tdiff %4 %3)))
    )))

;; Negs test
(clojure.test/function? 1)
(time
  (doall
  (pmap
(fn [c] (lfx-neg-s c
     [#(and (> (:value %1) (:value %2)) (= 3 (tdiff %2 %1)))
        #(and (> (:value %3) 2900) (= 101 (tdiff %3 %2)))]
           #(and    (> (:value %3) 2100) (> 4 (tdiff %2 %3))
    )))
 (vals (group-by :cust-id coll1000))))
  )


(time
  (def res
  (pmap
(fn [c] (lfx-neg-s
          ;lfred3neg
          c
        #(and
                       ;(= 10 (:id %1))
                        (> (:value %1) (:value %2))
                        (= 28 (tdiff %1 %2)))
     #(and (> (:value %3) (:value %2)) (< 19 (tdiff %2 %3)))
           #(and    (> (:value %4) 2000) (= 2 (tdiff %4 %3))
    )))
 (vals (group-by :cust-id coll1000)))))


(lfred3neg coll-date
        #(and
                       ;(= 10 (:id %1))
                        (> (:value %1) (:value %2))
                        (= 28 (tdiff %1 %2)))
     [#(and (= (:value %3) (:value %2)) (> 19 (tdiff %2 %3)))
        #(and (= (:value %4) (:value %3)) (= 1 (tdiff %3 %4)))]
           #(and  (> (:value %4) 2000) (= 2 (tdiff %4 %3))
    ))

;;; UTILS

(defn lf-non-empty [res] (filter not-empty (map #(filter not-empty %) res)))
(defn lf-group-by-hits-count [res] (filter #(< 0 (first %)) (group-by #(count (mapcat identity (vals %))) res))); groups results by counts without empty hits
(defn lf-summary-by-hits [res] (map #(hash-map (first %) (count (second %))) (group-by #(count (mapcat identity (vals %))) res)))
;this shouldn't be a part of utils becasue it's not generic enough. It's capable to get a single id value from each group an therefore it could only be used for repeated values inn each group (i.e. cust-id)
(defn lf-find-matched-values [k res] (remove nil? (map #(get-in (-> % first keys flatten first) [k]) (lf-non-empty res)))); matched values of k in res.

;; Checking results
t1b
(lf-non-empty t1)
(lf-non-empty t1b)
(lf-group-by-hits-count (mapcat identity t1)); lfx-gr requires mapcat idenntity
(lf-group-by-hits-count t1b)
(lf-summary-by-hits t1b)
(lf-summary-by-hits (mapcat identity t1))
(lf-find-matched-values :cust-id t1)


; When grouping input data the output gets divided into vectors for each group so it's necessary to flatten one level
(map vals (mapcat identity res))
(count (filter #(not-empty (mapcat identity (vals %))) (mapcat identity t1b)))

;; TODO: cleanup needed
(group-by #(count (mapcat identity (vals %))) res)
(count res)
; When grouping input data the output gets divided into vectors for each group so it's necessary to flatten one level
(map #(filter not-empty %) res);this is not enough...
(filter not-empty (map #(filter not-empty %) res));this filters out rsults
(def fil (filter not-empty (map #(filter not-empty %) res)))
(map count fil); how many hits
;(get-in (-> fil first first keys flatten first) [:cust-id])
(map #(get-in (-> % first keys flatten first) [:cust-id]) fil); returns cust-ids
(map #(group-by (fn [x] (count (mapcat identity (vals x)))) %) res);works almost OK...
(group-by #(count (mapcat identity (vals %))) (mapcat identity res));works OK. How many hits

(take 1 (filter not-empty (map #(filter not-empty %) t1)))
(remove nil? (map #(get-in (-> % first keys flatten first) [:cust-id]) (filter not-empty (map #(filter not-empty %) t1)))); matched  cust-id values
(filter #(< 0 (first %)) (group-by #(count (mapcat identity (vals %))) (mapcat identity t1)));groups results by counts

(map vals (mapcat identity res))
(filter #(not-empty (mapcat identity (vals %))) (mapcat identity res))
(group-by #(count (mapcat identity (vals %))) t); works OK. How many hits

(take 10 t)
(filter #(not-empty (vals %)) t); works. returns non emtpy
(filter #(= 1 (count (mapcat identity (vals %)))) t)
;(def fil2 (filter #(not-empty (vals %)) t))
(def fil2 (filter #(not-empty (vals %)) t))
;(get-in (-> fil2 first keys flatten first) [:cust-id]);works
(map #(get-in (-> % keys flatten first) [:cust-id]) fil2)
(tdiff  (second coll1000) (first coll1000))
