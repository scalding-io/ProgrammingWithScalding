#!/bin/bash
mvn clean install

cat src/main/scala/elasticsearch/README.md
sleep 3

echo "Writing some data to Elastic Search"
sleep 1
java -cp target/chapter8-0-jar-with-dependencies.jar com.twitter.scalding.Tool elasticsearch.WriteToElasticExample --local

echo "Reading some data from Elastic Search"
sleep 1
java -cp target/chapter8-0-jar-with-dependencies.jar com.twitter.scalding.Tool elasticsearch.ReadFromElasticExample --local

echo "Data received"
echo "-------------"
more data/es-out.tsv
