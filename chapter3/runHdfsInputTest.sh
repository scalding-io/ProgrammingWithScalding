hadoop jar chapter3-0-jar-with-dependencies.jar  com.twitter.scalding.Tool hdfsInputTest --hdfs --input hdfs:///dataset/2014/01/01/ --output /result/
hadoop fs -cat /result/*