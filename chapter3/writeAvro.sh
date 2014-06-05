#!/bin/sh
echo "Writing Avro files from Scalding"

mvn clean install

echo "Writing uncompressed avro file.."
java -cp target/chapter3-0-jar-with-dependencies.jar com.twitter.scalding.Tool avro.WriteAvroExample --hdfs --output data/avro

echo "Using snappy compression.."
java -cp target/chapter3-0-jar-with-dependencies.jar com.twitter.scalding.Tool -Dmapred.output.compress=true -Davro.output.codec=snappy avro.WriteAvroExample --hdfs --output data/avrosnappy

echo "UNCOMPRESSED AVRO"
echo "-----------------"

ls -la data/avro/*.avro
head -n 1 data/avro/part*

echo "SNAPPY COMPRESSED AVRO"
echo "----------------------"

ls -la data/avrosnappy/*
head -n 1 data/avrosnappy/part*
