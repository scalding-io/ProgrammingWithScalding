echo This is a happy day. A day to remember > input.txt
hadoop fs -mkdir -p hdfs:///data/input hdfs:///data/output
hadoop fs -put input.txt hdfs:///data/input/

hadoop jar target/chapter2-0-jar-with-dependencies.jar com.twitter.scalding.Tool WordCountJob --hdfs --input hdfs:///data/input --output hdfs:///data/output