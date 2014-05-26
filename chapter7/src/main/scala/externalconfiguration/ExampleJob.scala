package externalconfiguration

import com.twitter.scalding._

/**
 * Using JobBase we keep a single responsibility in this Job class
 */
 class ExampleJob(args: Args) extends JobBase (args) {

   println("Loading configuration from " + getValue("cluster-config"))
   println("-----------------------------------------------------")
   println("MySQL => " + appConfig.getValue("mysql"))
   println("Solr  => " + appConfig.getValue("solr"))
   println("HBase => " + appConfig.getValue("hbase"))
   println()

  val simpleExample = IterableSource[(Int)](List(1,2,3), ('num))
      .read
      .write(Tsv("data/output.tsv"))

  }

