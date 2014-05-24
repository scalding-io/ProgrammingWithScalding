#!/bin/bash
mvn clean install

java -cp target/chapter5-0-jar-with-dependencies.jar com.twitter.scalding.Tool externaloperations.ExampleJob --local --input src/main/resources/logs.tsv --users src/main/resources/users.tsv --output data/externaloperations/

echo "Displaying results of data/externaloperations/logs-daily-visits.tsv"
echo "-------------------------------------------------------------------"
more data/externaloperations/logs-daily-visits.tsv

echo "Displaying results of data/externaloperations/logs-visits-per-day.tsv"
echo "---------------------------------------------------------------------"
more data/externaloperations/logs-visits-per-day.tsv
