package adtargeting

import com.twitter.scalding.{Tool, Args}
import org.slf4j.LoggerFactory
import org.apache.hadoop.util.ToolRunner

/**
 */
object Runner extends App {

  val runnerArgs = Args(args)
  val configuration = new org.apache.hadoop.conf.Configuration

  val log = LoggerFactory.getLogger(this.getClass.getName)

  log.info("Executing [CalculateDailyAdPoints] Job")
  ToolRunner.run(configuration, new Tool,
    (classOf[CalculateDailyAdPoints].getName :: runnerArgs.toList).toArray )

}
