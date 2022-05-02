# Linked Facts

Linked Facts is a lightweight library for analyzing events relationships. It provides:
- functional approach (no macros)
- lightweight design (minimum external dependencies)
- simple way of defining related events using built-in Clojure predicate functions 
- in-memory processing of event data stored in Clojure records or hash-maps using lazy or reducers-based functions

## Why Linked Facts?

The goal of this project was to provide results using a data structure of linked events that can easily be subject of further data processing.

Linked Facts library provides the following main functions for analyzing events
- lfx-lazy - for lazy processing
- lfx-gr - for processing source data grouped in smaller chunks. This is the recommended function for non-lazy processing of larger collections
- lfx-s - for processing events stored in a single collection

The way of using lfx-s and lfx-lazy is the same (besides the laziness aspect). The lfx-gr function expects the source collection to be a wrapper of the actual event data (for example in case of customer data one can group data by customer id - just like in the example below) and it returns grouped data.

In order to analyze events one needs to define criteria for events matching which is as simple as defining an anonymous predicate function, for example:

```clojure
(lfx-lazy
  src-coll
  #(and (> (:value %1) (:value %2)) (> (:value %2) 400) (= 7 (tdiff %1 %2)))
```

This function defines a matching criterion saying the value of the first transaction event is greater than the value of the second one, the value of the second one is greater than 400 and  the second event occurs 7 days after the first one. By default the tdiff comparison function compares the temporal difference in days between two events but it can also use a different time interval supported by the java.time API which has to be supplied as the third argument (see example.clj for details).

## Usage

Below examples assume src-colls a record (or map) with keys used in the function calls. Let’s assume it contains a simple record of customer transaction data and create some testing data (see the example.clj file for more details):

```clojure
(defrecord Fact [id cust-id date value]);records should have better performance than ordinary maps
(def src-coll (for [i (range 1 10001)] (linked_facts.core.Fact. i (+ 1 (rand-int 10)) (java.time.LocalDate/of 2018 (+ 1 (rand-int 12)) (+ 1 (rand-int 27))) (rand-int 3000))))

(clojure.pprint/pprint 
 (take 1
  (lfx-lazy
    src-coll
    #(and (= (:cust-id %1) (:cust-id %2)) (> (:value %1) (:value %2)) (> (:value %1) 400) (= 21 (tdiff %1 %2)))
    #(and (= (:cust-id %2) (:cust-id %3)) (> (:value %3) (:value %2)) (= 1 (tdiff %2 %3)))
    #(and (= (:cust-id %3) (:cust-id %4)) (> (:value %4) 200) (= 2 (tdiff %4 %3))))))
```

Example output showing the chain of linked facts corresponding to the filters defined in the above function call. In this case four events were matched at the last level:

```clojure
({[{:id 3,
    :cust-id 6,
    :date #object[java.time.LocalDate 0x38709177 "2018-12-23"],
    :value 1901}
   {:id 9559,
    :cust-id 6,
    :date #object[java.time.LocalDate 0x10ed04ca "2018-12-02"],
    :value 915}
   {:id 9470,
    :cust-id 6,
    :date #object[java.time.LocalDate 0x36e552e2 "2018-12-01"],
    :value 1102}]
  ({:id 59,
    :cust-id 6,
    :date #object[java.time.LocalDate 0x10ed04ca "2018-12-03"],
    :value 1910}
   {:id 8947,
    :cust-id 6,
    :date #object[java.time.LocalDate 0x314a168a "2018-12-03"],
    :value 2061}
   {:id 7837,
    :cust-id 6,
    :date #object[java.time.LocalDate 0x1b948363 "2018-12-03"],
    :value 2467}
   {:id 3160,
    :cust-id 6,
    :date #object[java.time.LocalDate 0x332c1387 "2018-12-03"],
    :value 1275})})
```

Here’s a example of grouping the event data for analysis which is recommended for larger datasets:

```clojure
(def t1 (doall
 (pmap
  (fn [cust-data]
    (lfx-gr cust-data
     #(and (> (:value %1) (:value %2)) (= 7 (tdiff %1 %2)))
     #(and (> (:value %2) (:value %3)) (= 7 (tdiff %2 %3)))
     #(and (> (:value %4) 2000) (= 29 (tdiff %4 %3)))
     #(and (> (:value %5) 1900) (= 10 (tdiff %4 %5)))))
  (vals (group-by :cust-id src-coll)))))
```

The results in the above case will have the same overall structure, but the original grouping will be preserved which results in the nesting being one level deeper (so you might need to call mapcat identity on the result set to get rid of the grouping when it’s not needed). Example output:

```clojure
({[{:id 39905,
    :cust-id 487,
    :date #object[java.time.LocalDate 0x395b1590 "2018-06-10"],
    :value 2254}
   {:id 23283,
    :cust-id 487,
    :date #object[java.time.LocalDate 0x3f0cb888 "2018-06-03"],
    :value 1946}
   {:id 9967,
    :cust-id 487,
    :date #object[java.time.LocalDate 0x3e9488b "2018-05-27"],
    :value 796}
   {:id 4196,
    :cust-id 487,
    :date #object[java.time.LocalDate 0x7655e5eb "2018-06-25"],
    :value 2258}]
  ({:id 65791,
    :cust-id 487,
    :date #object[java.time.LocalDate 0x593c6450 "2018-06-15"],
    :value 2997})})
```

## Dependencies

```
<dependency>
  <groupId>linked-facts</groupId>
  <artifactId>linked-facts</artifactId>
  <version>0.2.0-SNAPSHOT</version>
</dependency>
```

```
[linked-facts "0.2.0-SNAPSHOT"]
```
## Author

Please contact me if you have any question or feedback.

Piotr Nowara 

piotrnowara[at]gmail.com

https://www.linkedin.com/in/piotr-nowara-35040121/
