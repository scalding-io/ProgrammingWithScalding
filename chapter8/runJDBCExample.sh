#!/bin/bash
mvn clean install

cat src/main/scala/jdbc/README.md
sleep 3

java -cp target/chapter8-0-jar-with-dependencies.jar com.twitter.scalding.Tool jdbc.JDBCExample --hdfs

echo "Data comming out of the MySQL database"
echo "--------------------------------------"
more data/part-00000
