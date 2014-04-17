package kmeans

import org.slf4j.LoggerFactory
import com.twitter.scalding.{Tool, Args}
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration

object KMeansRunner extends App {

  val mArgs = Args(args)
  val configuration: Configuration = new Configuration

  val log = LoggerFactory.getLogger(this.getClass.getName)

  log.info("Executing Job-1 [Scalding]")
  ToolRunner.run(configuration, new Tool,
    (classOf[ETLJob].getName :: mArgs.toList).toArray )

  log.info("Executing Job-2 [Mahout random centroid]")
  RandomCentroid.main(null)

  log.info("Executing Job-3 [Mahout K-Means]")
  KMeans.main(null)

  log.info("Executing Job-4 [Scalding]")
  ToolRunner.run(configuration, new Tool,
    (classOf[FinalJob].getName :: mArgs.toList).toArray )

}
