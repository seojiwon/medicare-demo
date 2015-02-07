Finding Anomalies in Medicare-B Data Set
==================

This demonstrates finding anomalies in Medicare-B data set using personalized PageRank algorithm.
We use Hadoop Pig and SociaLite (open-source graph analysis platform) to preprocess the data and analyze the data.
The details of the algorithm is described in Hortonworks blog (XXX: link here?)


Run
====

* run1.sh
 - Performs preprocessing. It uses Pig scripts in pre-process/ to clean the data and generate NPI similarity graph.

* run2.sh
 - Runs personalized PageRank algorithm to find anomalies in the data set.
   SociaLite script (find-anomalies.py) implements the algorithm.


Configuration
====
