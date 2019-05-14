# linked-facts

Linked Facts is a lightweight library for analyzing events relationships. It provides:
- functional approach (no macros)
- lightweight design (no external dependencies)
- simple way of defining related events using built-in Clojure predicate functions 
- in-memory processing of event data stored in Clojure records or hash-maps using lazy or reducers-based functions

## Why Linked Facts?

The goal of this project was to provide results using a data structure of linked events that can easily be subject of further data processing.

Linked Facts library provides the following main functions for analyzing events
- lfx-lazy - for lazy processing
- lfx-gr - for processing source data grouped in smaller chunks. This is the recommended function for non-lazy processing of large - collections
- lfx-s - for processing events stored in a single collection

The way of using lfs-b and lfx-lazy is the same. The lfx-gr function expects the source collection to be a wrapper of the actual event data (for example in case of customer data one can group data by customer id - just like in the example below) and it returns grouped data.

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
(defrecord Fact [id cust-id date value])
(def src-coll (for [i (range 1 10001)] (linked_facts.core.Fact. i (+ 1 (rand-int 10)) (f/parse (str "2018-" (+ 1 (rand-int 12)) "-" (+ 1 (rand-int 27)))) (rand-int 3000))))

(take 1
  (lfx-lazy
    src-coll
    #(and (= (:cust-id %1) (:cust-id %2)) (> (:value %1) (:value %2)) (> (:value %1) 400) (= 21 (tdiff %1 %2)))
    #(and (= (:cust-id %2) (:cust-id %3)) (> (:value %3) (:value %2)) (= 1 (tdiff %2 %3)))
    #(and (= (:cust-id %3) (:cust-id %4)) (> (:value %4) 200) (= 1 (tdiff %4 %3)))))
```

Example output:

```clojure
({[{:id 1,
    :cust-id 8,
    :date
    #object[org.joda.time.DateTime 0x11a65a0e "2017-07-16T00:00:00.000Z"],
    :value 1125}
   {:id 8161,
    :cust-id 8,
    :date
    #object[org.joda.time.DateTime 0x7437404b "2017-08-06T00:00:00.000Z"],
    :value 651}
   {:id 9586,
    :cust-id 8,
    :date
    #object[org.joda.time.DateTime 0x1c284680 "2017-08-07T00:00:00.000Z"],
    :value 2500}]
  ({:id 8161,
    :cust-id 8,
    :date
    #object[org.joda.time.DateTime 0x7437404b "2017-08-06T00:00:00.000Z"],
    :value 651}
   {:id 6958,
    :cust-id 8,
    :date
    #object[org.joda.time.DateTime 0x25200dee "2017-08-06T00:00:00.000Z"],
    :value 233}
   {:id 4418,
    :cust-id 8,
    :date
    #object[org.joda.time.DateTime 0x6f5456e "2017-08-06T00:00:00.000Z"],
    :value 279})})
```

Here’s a example of grouping the event data for analysis which is recommended for larger datasets.

```clojure
(def t1 (doall
 (pmap
  (fn [cust-data]
    (lfx-gr cust-data
     #(and (> (:value %1) (:value %2)) (= 7 (tdiff %1 %2)))
     #(and (> (:value %2) (:value %3)) (= 7 (tdiff %2 %3)))
     #(and (> (:value %4) 2000) (= 29 (tdiff %4 %3)))
     #(and (> (:value %5) 2900) (= 10 (tdiff %4 %5)))))
  (vals (group-by :cust-id src-coll)))))
```

The results in the above case will have the same overall structure, but the original grouping will be preserved which results in the nesting being one level deeper (so you might need to call mapcat identity on the result set to get rid of the grouping when it’s not needed). Example output:

```clojure
{[{:id 61,
     :cust-id 7,
     :date
     #object[org.joda.time.DateTime 0x5dd6bd79 "2017-02-11T00:00:00.000Z"],
     :value 1488}
    {:id 8296,
     :cust-id 7,
     :date
     #object[org.joda.time.DateTime 0x2183cbb9 "2017-02-18T00:00:00.000Z"],
     :value 555}
    {:id 2450,
     :cust-id 7,
     :date
     #object[org.joda.time.DateTime 0x75465746 "2017-02-25T00:00:00.000Z"],
     :value 20}
    {:id 3111,
     :cust-id 7,
     :date
     #object[org.joda.time.DateTime 0x5b043bc6 "2017-01-27T00:00:00.000Z"],
     :value 2247}]
   ({:id 2377,
     :cust-id 7,
     :date
     #object[org.joda.time.DateTime 0x33d168aa "2017-02-06T00:00:00.000Z"],
     :value 2919})}
```

## Dependencies

```
<dependency>
  <groupId>linked-facts</groupId>
  <artifactId>linked-facts</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```
[linked-facts "0.1.0-SNAPSHOT"]
```
