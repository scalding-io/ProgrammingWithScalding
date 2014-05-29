package coordinating

import com.twitter.scalding._

/**
 * Another dummy job
 */
class JobB(args: Args) extends Job(args) {

  // can still access property from appConfig.getValue("hbase")) if we extend
  // <externalconfiguration.JobBase> instead of <com.twitter.scalding.Job>

  IterableSource[(String)](List(("b")), ('name)).read
    .write(Tsv("data/output.tsv"))

}