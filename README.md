# Test task

## Task
Say we have an e-commerce site with products divided into categories like toys, electronics etc. We receive events like product was seen (impression), product page was opened, product was purchased etc. 

### Task #1: 
Enrich incoming data with user sessions. Definition of a session: for each user, it contains consecutive events that belong to a single category  and are not more than 5 minutes away from each other. Output should look like this:
```eventTime, eventType, category, userId, …, sessionId, sessionStartTime, sessionEndTime```  
Implement it using 1) sql window functions and 2) Spark aggregator.

### Task #2:
Compute the following statistics:
* For each category find median session duration
* For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins
* For each category find top 10 products ranked by time spent by users on product pages - this may require different type of sessions. For this particular task, session lasts until the user is looking at particular product. When particular user switches to another product the new session starts. 

## Implementation

### Task #1

#### Implementation

This task has been implemented using two different approaches:
* SQL Window Functions (implemented in [SqlWindowSessionEnricher](src/main/scala/com/sokeran/interview/enrichment/SqlWindowSessionEnricher.scala))
* Spark Aggregator (function implemented in [CalculateSessionIdFunction](src/main/scala/com/sokeran/interview/enrichment/CalculateSessionIdFunction.scala),
                    usage in [SqlAggregatorSessionEnricher](src/main/scala/com/sokeran/interview/enrichment/SqlAggregatorSessionEnricher.scala))

Both of this implementation have same trait [SessionEnricher](src/main/scala/com/sokeran/interview/enrichment/SessionEnricher.scala) to make it easier to test both of this implementation
and make it easier to use them interchangeably.

#### Testing

Both implementation have same test cases which can be found in [EnricherAbstractTest](src/test/scala/com/sokeran/interview/enrichment/EnricherAbstractTest.scala) - 
basic test for both implementation which can be found in [SqlWindowSessionEnricherTest](src/test/scala/com/sokeran/interview/enrichment/SqlWindowSessionEnricherTest.scala) for
SQL Window Function implementation and in [SqlAggregatorSessionEnricherTest](src/test/scala/com/sokeran/interview/enrichment/SqlAggregatorSessionEnricherTest.scala) for
Spark Aggregator implementation
Files which are used for testing are present in `test/resources` folder

### Task #2

#### Implementation

All tasks are implemented using raw SQL queries and `DataFrame`. 
All methods can be found in [SessionStatsCalculator](src/main/scala/com/sokeran/interview/stats/SessionStatsCalculator.scala)

#### Testing

All tests can be found in single file [SessionStatsCalculatorTest](src/test/scala/com/sokeran/interview/stats/SessionStatsCalculatorTest.scala)
File `all.csv` which are used for testing is present in `test/resources` folder

### Running

#### Scripts

* Build script is present in [build.sh](build.sh) which should be used to build application and create
fat-jar which will be used for app execution
* Run script is present in [run.sh](run.sh) which runs [App](src/main/scala/com/sokeran/interview/App.scala)

#### Arguments

All constants provided in tasks can be adjusted using following arguments:
* -i, --input - input path to file with data

* -o, --output-folder - path to output folder 

* (optional, default 300s) -sl, --session-length - Session length in seconds (max delay between consequent requests in single session)

* (optional, default 60s) -g1, --session-group-1 - Session length for session group 1 (task 2.2)

* (optional, default 300s) -g2, --session-group-2 - Session length for session group 2 (task 2.2)

* (optional, default 10) -t, --top-products - Count of top products that must be shown for top products per category


#### TODO

* Currently all tasks are implemented in single file [App](src/main/scala/com/sokeran/interview/App.scala) what is not
 as convenient as it would be if all tasks were present in separate files
 
* Write more advanced tests to test all corner cases correctly and thoroughly

* etc.
