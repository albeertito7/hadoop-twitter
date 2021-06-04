# hadoop-twitter

MapReduce application to calculate the trending topics on twitter and feelings associated with such hashtags

All coding is done in Java using the MapReduce framework, and the solution is structured by packages as follows
* **cleanUp** => contain the clean up mappers awa a driver to run it regardless
* **topN** => contain the topN mapper and reducer, and a driver regardless the main application
* **trendingTopics** => contain the reducer and two mappers, one working with JSON format used for the main application, and other working with TXT files which is used by the specific driver to get the trending topics via regex hashtag pattern
* **main** => contain the main application driver job control resulting as CleanUp => TrendingTopics => TopN
* **sentimentAnalysis** => contain the application to extract the feelings based on a cleanUP JSON file

>Note: The mapreduce appointment will work with twitter data in text and json format

<img src="assets/hadoop-logo.png" width="580" />
