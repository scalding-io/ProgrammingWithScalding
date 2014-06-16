#!/bin/bash
mvn clean install

export DRIVEN_API_KEY=D991A15E7A174E098900CDEE4F3A3CA6

# We will now execute a 'slim' jar that is just 15 KBytes
MAIN_JAR=target/chapter7-0.jar

# Aggregate all dependencies into the HADOOP_CLASSPATH
for f in alternateLocation/*.jar; do CP=$f:$CP; done
export HADOOP_CLASSPATH=$MAIN_JAR:$CP

# The following block does not need to be executed every time
# as our dependencies don't change frequently 
hadoop fs -mkdir -p project1/libs/
hadoop fs -rm -skipTrash project1/libs/*
hadoop fs -put alternateLocation/* project1/libs/
hadoop fs -put $MAIN_JAR project1/libs/

# Runs : a Scalding Job | A Scala application | and then another Scalding Job
hadoop jar $MAIN_JAR slimjar.JobRunner slimjar.ExampleJob --hdfs --dependencies project1/libs/ --heapInc
