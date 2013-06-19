(ns cascalog-workshop.core
  "This code was copied almost verbatim from the
  `cascalog.koans.basic-playground-queries` namespace of Sam Ritchie's
  [cascalog-koans](https://github.com/sritchie/cascalog-koans) project.

  Thanks to Sam Ritchie for generously granting me permission to use
  this material!"
  (:require [cascalog.api :refer :all]
            [cascalog.playground :refer :all]
            [cascalog.ops :as ops]))

;; Here are some basic queries to get you all started. These make use
;; of Cascalog's playground dataset, located in the Cascalog codebase
;; at `cascalog.playground`.
;;
;; This first query returns all people from the age dataset under 30:

(def people-under-30
  (<- [?person]
      (age ?person ?age) ;; <-- generator
      (< ?age 30)))      ;; <-- filter

;; This query returns the same, this time pairing users with age:

(def under-30-with-age
  (<- [?person ?age]
      (age ?person ?age) ;; <-- generator
      (< ?age 30)))      ;; <-- filter

;; The second predicate introduces a constraint that ?n must equal its
;; square. This is only true for 0 and 1:

(def square-equals-self
  (<- [?n]
      (integer ?n)      ;; <-- generator
      (* ?n ?n :> ?n))) ;; <-- operation

;; This query constrains ?n to equal its own cube:

(def cubed-equals-self
  (<- [?n]
      (integer ?n)         ;; <-- generator
      (* ?n ?n ?n :> ?n))) ;; <-- operation

;; This query returns all users following someone younger.

(def follows-younger
  (<- [?person1 ?person2]
      (age ?person1 ?age1)        ;; <-- generator
      (follows ?person1 ?person2) ;; <-- generator
      (age ?person2 ?age2)        ;; <-- generator
      (< ?age2 ?age1)))           ;; <-- filter

;; ## Aggregator implementations
;;
;; `always-one` the initialization function for each of our tuples in
;; the `our-count` aggregator. Because the `count` predicate doesn't
;; take any input variables -- remember, (our-count ?count) -- the
;; initialization variable shouldn't take any inputs.
;;
;; To count items, assign 1 to each item.

(defn always-one [] 1)

(defparallelagg our-count
  "Parallel \"count\" aggregator. Converts tuple in its input group
  into a 1, adds all 1s together in parallel."
  :init-var    #'always-one
  :combine-var #'+)

(defparallelagg our-sum
  "Parallel \"sum\" aggregator, meant to act on numbers. Adds all
  numbers in its group together."
  :init-var    #'identity
  :combine-var #'+)

;; aggregate-op implementation of distinct-count.

(defaggregateop distinct-count*
  ([] [nil 0])
  ([[prev count] val]
     [val (if (= prev val)
            count
            (inc count))])
  ([[recent count]] [count]))

(def distinct-count
  (<- [!val :> !count]
      (:sort !val)
      (distinct-count* !num :> !sum)))

;; ### Defbufferop discussion
;;
;; The following points are a bit subtle, and involve a behavior of
;; debufferop that's similar to defmapcatop.

(def test-src
  [[1 2]
   [1 3]
   [1 4]
   [2 1]])

(defbufferop tuples->str-1
  "Returns a sequence of string tuple fields."
  [tuple-seq]
  (map str tuple-seq))

(?<- (stdout)
     [?x ?str]
     (test-src ?x ?y)
     (tuples->str-1 ?y :> ?str))

;; RESULTS
;; -----------------------
;; 1	(2)
;; 1	(3)
;; 1	(4)
;; 2	(1)
;; -----------------------

(defbufferop tuples->str-2
  "Returns a single tuple with one field that contains a sequence."
  [tuple-seq]
  [[(map str tuple-seq)]])

(?<- (stdout)
     [?x ?str]
     (test-src ?x ?y)
     (tuples->str-2 ?y :> ?str))

;; RESULTS
;; -----------------------
;; 1	("(2)" "(3)" "(4)")
;; 2	("(1)")
;; -----------------------

(def test-tap
  [["a" 1]
   ["b" 2]
   ["a" 3]])

;; defbufferop's meant to return something sort of like
;; defmapcatop. Is this a bug? Is this a feature?

(defbufferop dosum [tuples]
  [(reduce + (map first tuples))])

(?<- (stdout)
     [?a ?sum]
     (test-tap ?a ?b)
     (dosum ?b :> ?sum))

;; RESULTS
;; -----------------------
;; a	4
;; b	2
;; -----------------------

(def our-avg
  (<- [?x :> ?avg]
      (our-sum ?x :> ?sum)
      (our-count ?count)
      (/ ?sum ?count :> ?avg)))

;; Using our average.

(let [src [[1] [2] [1]]]
  (?<- (stdout)
       [?avg]
       (src ?x)
       (our-avg ?x :> ?avg)))

;; Custom aggregator for fires objects, represented here with Clojure
;; maps.

(def fire-src
  [["a" {:count 1
         :above-conf 1
         :above-temp 1
         :both-preds 1}]

   ["a" {:count 3
         :above-conf 1
         :above-temp 0
         :both-preds 0}]])

(defn combine-fires* [m1 m2]
  (merge-with + m1 m2))

(defparallelagg combine-fires
  :init-var    #'identity
  :combine-var #'combine-fires*)

(?<- (stdout)
     [?letter ?combined]
     (fire-src ?letter ?fire)
     (combine-fires ?fire :> ?combined))

(defparallelagg our-max
  :init-var    #'identity
  :combine-var #'max)

(defparallelagg our-min
  :init-var    #'identity
  :combine-var #'min)

(def people-under-30-count
  (<- [?count]
      (age _ ?age)       ;; <-- generator
      (< ?age 30)        ;; <-- operation
      (our-count ?count))) ;; <-- aggregator

(def follows-count
  (<- [?person ?count]
      (follows ?person _)  ;; <-- generator
      (ops/count ?count))) ;; <-- aggregator

;; Word Counting

(defmapcatop split [sentence]
  (.split sentence "\\s+"))

(def wordcount-query
  (<- [?word ?count]
      (sentence ?sentence)       ;; <-- generator
      (split ?sentence :> ?word) ;; <-- operation
      (ops/count ?count)))       ;; <-- aggregator

(def a-follows-b
  (let [many-follows (<- [?person]
                         (follows ?person _) ;; <-- generator
                         (ops/count ?count)  ;; <-- aggregator
                         (> ?count 2))]      ;; <-- filter
    (<- [?person1 ?person2]
        (many-follows ?person1)        ;; <-- generator
        (many-follows ?person2)        ;; <-- generator
        (follows ?person1 ?person2)))) ;; <-- generator

(def inner-join
  (<- [?person ?age ?gender]
      (age ?person ?age)         ;; <-- generator
      (gender ?person ?gender))) ;; <-- generator

;; very similar to outer-join:

(def outer-join
  (<- [?person !!age !!gender]
      (age ?person !!age)         ;; <-- generator
      (gender ?person !!gender))) ;; <-- generator
