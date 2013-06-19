(ns cascalog-workshop.demo
  "An example Cascalog workflow that computes climatological statistics
  from monthly historical weather data recorded by the United States
  Historical Climatology Network (USHCN)."
  (:require [cascalog.api :refer :all]
            [clojure.string :refer [trim]]
            [clojure.tools.logging :as log])
  ; The (:gen-class) form is required to generate a static public main
  ; function for Hadoop.
  (:gen-class))

(defn string->float
  "Parse a string to a float, or to nil if the string represents a
  missing value."
  [string]
  (let [missing (Float. "-9999")
        value (-> string (trim) (Float.))]
    (if (= value missing)
      nil
      value)))

(defn parse-line
  "Parse monthly weather data from a USHCN-formatted fixed-width text
  line into a hash-map, omitting any missing values.

  Quoting from the
  [documentation](ftp://ftp.ncdc.noaa.gov/pub/data/ushcn/v2/monthly/readme.txt):

    Each record (line) in the files contains one year of 12 monthly values plus an
    annual value (note that the uncertainty estimates have no annual value).
    The values on each line include the following:

    ------------------------------
    Variable   Columns   Type
    ------------------------------
    STATION ID     1-6   Character
    ELEMENT        7-7   Integer
    YEAR          8-11   Integer
    VALUE1       13-17   Integer
    FLAG1        18-18   Character
    VALUE2       20-24   Integer
    FLAG2        25-25   Character
      .           .          .
      .           .          .
      .           .          .
    VALUE13     97-101   Integer
    FLAG13     102-102   Character
    ------------------------------

    These variables have the following definitions:

    ID         is the station identification code.  Please see \"ushcn-stations.txt\"
               for a complete list of stations and their metadata.

    ELEMENT    is the element code.  There are four values corresponding to the
               element contained in the file:

               1 = mean maximum temperature (in tenths of degrees F)
               2 = mean minimum temperature (in tenths of degrees F)
               3 = average temperature (in tenths of degrees F)
               4 = total precipitation (in hundredths of inches)

    YEAR       is the year of the record.

    VALUE1     is the value for January in the year of record (missing = -9999).

    FLAG1      is the flag for January in the year of record.  There are
               five possible values:

               Blank = no flag is applicable;

               E     = value is an estimate from surrounding values; no original
                       value is available;
               I     = monthly value calculated from incomplete daily data (1 to 9
                       days were missing);
               Q     = value is an estimate from surrounding values; the original
                       value was flagged by the monthly quality control algorithms;
               X     = value is an estimate from surrounding values; the original
                       was part of block of monthly values that was too short to
                       adjust in the temperature homogenization algorithm.

    VALUE2     is the value for February in the year of record.

    FLAG2      is the flag for February in the year of record.
      .
      .
      .
    VALUE12    is the value for December in the year of record.

    FLAG12     is the flag for December in the year of record.

    VALUE13    is the annual value (mean for temperature; total for precipitation)

    FLAG13     is the flag for the annual value.
  "
  [line]
  (log/infof "This is a log message: %s" line)
  (let [id           (subs line 0 6)
        element      (condp = (subs line 6 7)
                       "1" :mean-maximum-temperature
                       "2" :mean-minimum-temperature
                       "3" :average-temperature
                       "4" :total-precipitation)
        year         (Integer. (subs line 7 11))
        months       (range 1 13)
        slices       (map #(vector % (+ 5 %)) (range 12 96 7))
        values       (map #(string->float (apply subs line %)) slices)]
    [id element year (into {} (filter val (zipmap months values)))]))

(defn parse-lines
  "Given a path to monthly historical weather data in USHCN-formatted
  fixed-width text lines, return a query that yields tuples containing
  monthly values for each weather station, element (measurement type),
  and year."
  [path]
  (let [source (hfs-textline path)]
    (<- [?id ?element ?year ?monthly-values]
        (source :> ?line)
        (parse-line ?line :> ?id ?element ?year ?monthly-values))))

(defaggregateop compute-monthly-means*
  "Given tuples containing monthly historical weather records for a
  weather station, compute a nested hash-map of climatological means by
  month."
  ; Initialize a pair of state maps. The first map maintains running
  ; totals (numerators), and the second map maintains running counts
  ; (denominators).
  ([]
   [{} {}])
  ; Add each [element year monthly-values] tuple to the running totals
  ; and counts.
  ([state element year monthly-values]
   (let [[totals counts] state
         ones            (zipmap (keys monthly-values) (repeat 1))]
     [(update-in totals [element] #(merge-with + % monthly-values))
      (update-in counts [element] #(merge-with + % ones))]))
  ; Compute the final output tuple from the accumulated state. The
  ; climatological mean is computed by dividing each numerator by its
  ; corresponding denominator.
  ([state]
   (let [[totals counts] state
         elements (keys totals)]
     (log/infof "This is a log message: %s" counts)
     [(into {}
            (map #(vector % (merge-with / (totals %) (counts %)))
                 elements))])))

(defn compute-monthly-means
  "Given a source of monthly historical weather records, return a query
  that yields climatological statistics for each id."
  [monthly-records]
  (<- [?id ?statistics]
      (monthly-records :> ?id ?element ?year ?monthly-values)
      (compute-monthly-means* ?element ?year ?monthly-values :> ?statistics)))

(defn -main
  "Read USHCN monthly historical weather data from a source path,
  compute monthly climatological statistics for each weather station,
  and write the statistics to a sink tap. The output statistics can be
  plotted as a [climograph](http://en.wikipedia.org/wiki/Climograph).

  The following command will run this main class:

    hadoop jar path/to/uberjar.jar \\
        cascalog_workshop.demo \\
        scheme://input/uri scheme://output/uri

  `uberjar.jar` is the JAR file that is built by `lein uberjar`.

  Note that the fully-qualified Java name
  `cascalog_workshop.uschn.ClimographStatistics` contains an underscore
  in place of a hyphen."
  [input-path output-path]
  (let [output-sink (hfs-textline output-path)]
    (?- output-sink
        (compute-monthly-means (parse-lines input-path)))))
