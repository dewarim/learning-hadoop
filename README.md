# Learning Hadoop

This repository hosts a collection of classes I use to implement stuff I learned from some chapters of "Hadoop: The 
definitive Guide, 4th.ed" by Tom White. It is based on Hadoop 2.7.1 and contains configuration files (under 
src/main/resources) for a local pseudo-cluster.

# Usage

It contains some example apache log data to use when experimenting with a simple log analyzer. You can import this
into your own HDFS before running the LogDriver example:

    # in the project folder:
    mvn package
    # you may have to adjust the following path:
    export HADOOP_CLASSPATH=target/learning-hadoop.jar
    hadoop fs -copyFromLocal data/logs  /user/$USER/input/logs
    hadoop de.dewarim.learning.hadoop.LogDriver input/logs/fantasy_connector_de.log output/o1
    hadoop fs -cat /user/ingo/output/o1/part-r-00000 | less

# License
 
Gnu Public License 3.0 for my own code.
 
