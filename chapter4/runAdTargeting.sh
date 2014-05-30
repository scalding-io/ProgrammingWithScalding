#!/bin/bash
mvn clean install

export DRIVEN_API_KEY=D991A15E7A174E098900CDEE4F3A3CA6

java -cp target/chapter4-0-jar-with-dependencies.jar -Xmx10G adtargeting.Runner --input log_file.tsv --local

