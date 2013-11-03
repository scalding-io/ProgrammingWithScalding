echo "first line" > 1
echo "second line" > 2
echo "third line" > 3
hadoop fs -mkdir -p hdfs:///input
hadoop fs -put 1 hdfs:///input/
hadoop fs -put 2 hdfs:///input/
hadoop fs -put 3 hdfs:///input/

hadoop jar target/chapter3-0-jar-with-dependencies.jar com.twitter.scalding.Tool flatMap --hdfs --input hdfs:///input --output hdfs:///output