#!/bin/bash
mvn clean install

java -cp target/chapter5-0-jar-with-dependencies.jar com.twitter.scalding.Tool dependencyinjection.ExampleJob --local --input src/main/resources/logs.tsv --users src/main/resources/users.tsv --output data/dependencyinjection.tsv

echo "Displaying results of data/dependencyinjection.tsv"
echo "--------------------------------------------------"
more data/dependencyinjection.tsv
