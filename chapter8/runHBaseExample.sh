#!/bin/bash
mvn clean install

cat src/main/scala/hbasespyglass/README.md
sleep 3

java -cp target/chapter8-0-jar-with-dependencies.jar com.twitter.scalding.Tool hbasespyglass.HBaseScanExample --hdfs

echo "Data comming out of HBase"
echo "--------------------------------------"
more data/hbase.tsv
