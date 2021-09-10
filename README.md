# hadoop-twitter

MapReduce application to calculate the trending topics on twitter and feelings associated with such hashtags.

All coding is done in Java using the MapReduce framework, and the solution is structured by packages as follows
| Package 	| Description	|
|:---	|:---	|
| cleanUp 	| contain the cleanUp mappers and a driver to run it regardless 	|
| topN 	| contain the topN mapper and reducer, and a driver regardless the main application 	|
| trendingTopics 	| contain the reducer and two mappers, one working with JSON format used for the main application, and the other with TXT files which is used by the specific driver to get thre trending topics via regez hashtag pattern 	|
| main 	| contain the main application driber job control resulting as **cleanUp => trendingTopics => topN** 	|
| sentimentAnalysis 	| contain the application to extract the feelings based on a cleanUP JSON file 	|                                                                          |

>Note: The mapreduce appointment will work with twitter data in text and json format

IntelliJ IDEA was used as IDE to develop the application, thus, in the file "hadoop-twitter.iml" you will find the following hadoop project dependencies:
  - hadoop-common
  - hadoop-mapreduce-client-jobclient
  - hadoopo-mapreduce-client-core

which use the version 2.7.3

***

>Note: If using Windows check how to install hadoop correctly at [link](https://gist.github.com/albeertito7/25a241972c469390f120fe353496874a).

<img src="assets/hadoop-logo.png" width="580" />
