#!/bin/bash
mvn clean install

export DRIVEN_API_KEY=D991A15E7A174E098900CDEE4F3A3CA6

java -cp target/chapter7-0-jar-with-dependencies.jar chainingjobs.ExampleRunner --local --cluster-config src/main/resources/dev.properties

echo "Displaying results of data/output.tsv"
echo "-------------------------------------"
more data/output.tsv
