package multipleenvironments

import com.twitter.scalding._

/**
 * Using JobBase we keep a single responsibility in this Job class
 */
 class ExampleJob(args: Args) extends JobBase (args) {

   println("Using MySQL: " + appConfig.getString("mysql"))
   println("Using Solr:  " + appConfig.getString("solr"))
   println("Using Memcache: " + appConfig.getString("memcache"))
   println("Using HBase: " + appConfig.getString("mysql"))

    val aList = List(1,2,3)
    val dropExample = IterableSource[(Int)](aList, ('num))
      .read
      .write(Tsv("Output-drop"))

  }

