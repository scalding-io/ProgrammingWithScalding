#!/bin/bash
mvn clean install

java -cp target/chapter5-0-jar-with-dependencies.jar com.twitter.scalding.Tool latebound.ExampleJob --local --input src/main/resources/logs.tsv --users src/main/resources/users.tsv --output data/latebound.tsv

echo "Displaying results of data/latebound.tsv"
echo "----------------------------------------"
more data/latebound.tsv
