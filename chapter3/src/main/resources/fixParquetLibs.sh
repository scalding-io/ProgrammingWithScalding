#!/bin/bash

echo "Fixing CDH lib/Parquet"
echo "----------------------"

cd /opt/cloudera/parcels/CDH-4.5.0-1.cdh4.5.0.p0.30/lib/parquet

wget http://central.maven.org/maven2/com/twitter/parquet-cascading/1.5.0/parquet-cascading-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-column/1.5.0/parquet-column-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-common/1.5.0/parquet-common-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-encoding/1.5.0/parquet-encoding-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-format/1.5.0/parquet-format-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-generator/1.5.0/parquet-generator-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-hadoop/1.5.0/parquet-hadoop-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-hadoop-bundle/1.5.0/parquet-hadoop-bundle-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-jackson/1.5.0/parquet-jackson-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-pig/1.5.0/parquet-pig-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-pig-bundle/1.5.0/parquet-pig-bundle-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-hive/1.5.0/parquet-hive-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-thrift/1.5.0/parquet-thrift-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-avro/1.5.0/parquet-avro-1.5.0.jar

mkdir BACKUP
mv -f *-cdh4.5.0.jar BACKUP/


echo "Fixing CDH lib/hadoop"
echo "---------------------"

cd /opt/cloudera/parcels/CDH-4.5.0-1.cdh4.5.0.p0.30/lib/hadoop/

mkdir BACKUP
mv original-parquet-* BACKUP/
mv parquet-* BACKUP/

wget http://central.maven.org/maven2/com/twitter/parquet-cascading/1.5.0/parquet-cascading-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-column/1.5.0/parquet-column-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-common/1.5.0/parquet-common-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-encoding/1.5.0/parquet-encoding-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-format/1.5.0/parquet-format-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-generator/1.5.0/parquet-generator-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-hadoop/1.5.0/parquet-hadoop-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-hadoop-bundle/1.5.0/parquet-hadoop-bundle-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-jackson/1.5.0/parquet-jackson-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-pig/1.5.0/parquet-pig-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-pig-bundle/1.5.0/parquet-pig-bundle-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-hive/1.5.0/parquet-hive-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-thrift/1.5.0/parquet-thrift-1.5.0.jar
wget http://central.maven.org/maven2/com/twitter/parquet-avro/1.5.0/parquet-avro-1.5.0.jar
