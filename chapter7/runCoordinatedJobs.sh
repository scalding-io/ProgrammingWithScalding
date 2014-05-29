#!/bin/bash
mvn clean install

export DRIVEN_API_KEY=D991A15E7A174E098900CDEE4F3A3CA6

# Runs : a Scalding Job | A Scala application | and then another Scalding Job
java -cp target/chapter7-0-jar-with-dependencies.jar coordinating.ExampleRunner --local
