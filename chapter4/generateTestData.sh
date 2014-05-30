#!/bin/bash
mvn clean install

export DRIVEN_API_KEY=D991A15E7A174E098900CDEE4F3A3CA6

# If you have plenty of RAM (i.e. 10 GB use the following command) you can generate the full 5 GB of the example
# java -cp target/chapter4-0-jar-with-dependencies.jar -Xmx10G com.twitter.scalding.Tool generatedata.CreateMockLogData --num.of.lines 35000000 --local

# By default execute with 4GB RAM
java -cp target/chapter4-0-jar-with-dependencies.jar -Xmx4G com.twitter.scalding.Tool generatedata.CreateMockLogData --num.of.lines 15000000 --local
