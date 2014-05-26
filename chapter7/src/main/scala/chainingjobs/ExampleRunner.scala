package chainingjobs

import org.slf4j.LoggerFactory
import com.twitter.scalding.{Tool, Args}
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration

/**
 * Every application will run in sequence in this file
 * i.e. first the Scala app and then one Scalding job
 */
object ExampleRunner extends App {

  val runnerArgs = Args(args)
  val configuration = new Configuration

  val log = LoggerFactory.getLogger(this.getClass.getName)

  log.info("Execute a [Scala] application")
  ScalaApp.main(null)

  log.info("Executing a [Scalding] job]")
  ToolRunner.run(configuration, new Tool,
    (classOf[ScaldingJob].getName :: runnerArgs.toList).toArray )

}
