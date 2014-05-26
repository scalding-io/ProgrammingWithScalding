package chainingjobs

import com.twitter.scalding._
import externalconfiguration.JobBase

/**
  */
class ScaldingJob(args: Args) extends JobBase(args) {

  println("HBase => " + appConfig.getValue("hbase"))

  val kidsList = List(("john"))

  val pipe =
    IterableSource[(String)](kidsList, ('name)).read
      .write(Tsv("data/output.tsv"))

}