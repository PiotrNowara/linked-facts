;; TODO - create a standalone clj files with more examples


;; TESTING

(defrecord Fact [id cust-id date value]);records should have better performance than ordinary maps
(def src-coll (for [i (range 1 100001)] (linked_facts.core.Fact. i (+ 1 (rand-int 500)) (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) (rand-int 3000))))


;lazy call
(clojure.pprint/pprint (take 1
  (lfx-lazy
    src-coll
    #(and (= (:cust-id %1) (:cust-id %2)) (> (:value %1) (:value %2)) (> (:value %1) 400) (= 21 (tdiff %1 %2)))
    #(and (= (:cust-id %2) (:cust-id %3)) (> (:value %3) (:value %2)) (= 1 (tdiff %2 %3)))
    #(and (= (:cust-id %3) (:cust-id %4)) (> (:value %4) 200) (= 1 (tdiff %4 %3))))))

;"batch" call
(def t1b
  (lfx-s
    src-coll
    #(and (= (:cust-id %1) (:cust-id %2)) (> (:value %1) (:value %2)) (< (:value %1) 1400) (= 21 (tdiff %1 %2)))
    #(and (= (:cust-id %2) (:cust-id %3)) (> (:value %3) (:value %2)) (= 1 (tdiff %2 %3)))
    #(and (= (:cust-id %3) (:cust-id %4)) (> (:value %4) 2000) (= 2 (tdiff %4 %3)))))

;"grouped" call
(def t1 (doall
 (pmap
  (fn [cust-data]
    (lfx-gr cust-data
     #(and (> (:value %1) (:value %2)) (= 7 (tdiff %1 %2)))
     #(and (> (:value %2) (:value %3)) (= 7 (tdiff %2 %3)))
     #(and (> (:value %4) 2000) (= 29 (tdiff %4 %3)))
     #(and (> (:value %5) 1900) (= 10 (tdiff %4 %5)))))
  (vals (group-by :cust-id src-coll)))))

;; RESULTS HANDLING

; get results
(def res (filter not-empty (map #(filter not-empty %) t1)))
; print 1st hit
(clojure.pprint/pprint (take 1 (mapcat identity res)))
; matched  cust-id values. collection needs to be filtered out of nulls
(remove nil? (map #(get-in (-> % first keys flatten first) [:cust-id]) res));
; groups results by counts without empty hits
(filter #(< 0 (first %)) (group-by #(count (mapcat identity (vals %))) (mapcat identity t1)))
; how many hits?
(map #(hash-map (first %) (count (second %))) (group-by #(count (mapcat identity (vals %))) (mapcat identity t1)))
