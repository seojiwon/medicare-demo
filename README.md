Finding Anomalies in Medicare-B Data Set
==================

This demonstrates finding anomalies in Medicare-B data set using personalized PageRank algorithm.
We use Hadoop Pig and SociaLite (open-source graph analysis platform) to preprocess the data and analyze the data.
The details of the algorithm is described in Hortonworks blog (XXX: link here?)


Installation
====
* Hadoop/Pig
 - 

* SociaLite
 - Edit conf/socialite-env.sh to change settings.

    Set JAVA_HOME to point to your Java installation path (e.g. /usr/lib/jvm/jdk1.7.0_51/)

    Set HADOOP_HOME to point to your Hadoop installation path.

    Set SOCIALITE_HEAPSIZE to change the heap size of SociaLite runtime. (e.g. 24000 for 24GB)

 -  Go to socialite/ directory and enter ant to build SociaLite (requires Apache-ant).

Run
====

Run these two scripts to preprocess the data and apply the algorithm.

* run1.sh
 - Performs preprocessing. It uses Pig scripts in pre-process/ to clean the data and generate NPI similarity graph.

* run2.sh
 - Runs personalized PageRank algorithm to find anomalies in the data set.
   SociaLite script (find-anomalies.py) implements the algorithm.


