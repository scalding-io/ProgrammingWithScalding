#!/bin/bash
mvn clean install

java -cp target/chapter9-0-jar-with-dependencies.jar com.twitter.scalding.Tool BookSimilarity --hdfs

echo "Data comming out of HBase"
echo "--------------------------------------"
echo "pipe : books =>"
more data/output-bs-books.tsv/part-*
echo "pipe : tf =>"
more data/output-bs-tf.tsv/part-*
echo "pipe : df =>"
more data/output-bs-df/part-*
echo "pipe : tfidf =>"
more data/output-bs-tfidf/part-*
echo "pipe : book-similarities =>"
more data/output-book-similarities.tsv/part-*
