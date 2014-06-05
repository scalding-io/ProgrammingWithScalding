#!/bin/sh
echo "Reading Avro files from Scalding"

mvn clean install

echo "Writing uncompressed avro file.."
java -cp target/chapter3-0-jar-with-dependencies.jar com.twitter.scalding.Tool avro.ReadAvroExample --local

echo "Read from Avro file"
echo "-------------------"
head -n 3 data/from-avro

echo "Read from Snappy compressed Avro file"
echo "-------------------------------------"
head -n 3 data/from-avrosnappy
