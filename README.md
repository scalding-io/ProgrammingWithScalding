Source code for PACKT Book 'Programming Map-Reduce With Scalding'
Page http://scalding-io.github.io/ProgrammingMapReduceWithScalding/

The book consists of 9 chapters 

* **Hadoop and Abstractions** - 
Introduction to Hadoop, Map Reduce, Pipelining, Cascading, Pig and Hive.
Chapter presents benefits of higher level abstractions of Map Reduce (concepts and capabilities).

* **Get ready for Scalding** -
Theory about Scalding - the Scala Domain Specific Language utilising Cascading. 
Development environment setup including local hadoop cluster for development.
Execute the first `Hello World` Scalding example.

* **Scalding reference examples** -
The core capabilities of scalding: i) Map-like functions, ii) Grouping/reducing functions iii) Join operations 

* **Advanced concepts-Intermediate examples** -
A Scalding log processing flow for a News company, aggregating multiple sources will be presented. 
Through an example with multiple pipe-lines some more advanced concepts are presented.

* **Development Patterns and Best Practices** -
Presents interesting design patterns to overcome common issues and add more capabilities into Scalding.
This chapter presents the External Operations pattern, the Dependency Injection pattern and the Late Bound Dependency pattern

* **Testing & TDD** -
Best practices of first defining behaviour (_Behaviour Driven Development_) then tests (_Test Driven Development_) and then completing the implementation. How to write unit, integration tests and also apply Black-box testing methodologies in the context of Big Data.

* **Running in Production** -
Introduction to Distributed Cache and Hadoop production system deployment and execution. 
Handling multiple HDFS sources: text, avro, csv, etc. 
How to monitor and maintain cluster stability.

* **External data stores** -
Interaction with external external SQL, NOSQL and in-memory applications like MemCache , Solr, ElasticSearch.
Code for accessing SQL databases 

* **Machine Learning** - 
Machine Learning and Recommendation systems - Pattern - PMML - Mahout.
An example in Scalding for processing visits to an e-commerce site with Mahout/Pattern 
to generate recommendations and store them into a real-time no-sql database.
