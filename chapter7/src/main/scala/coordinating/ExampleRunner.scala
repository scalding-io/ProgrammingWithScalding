package coordinating

import org.slf4j.LoggerFactory
import com.twitter.scalding.{Tool, Args}
import org.apache.hadoop.util.ToolRunner

/**
 * Every application will run in sequence in this file
 * i.e. first the Scala app and then one Scalding job
 */
object ExampleRunner extends App {

  val runnerArgs = Args(args)
  val configuration = new org.apache.hadoop.conf.Configuration

  val log = LoggerFactory.getLogger(this.getClass.getName)

  log.info("Executing a [Scalding] Job - A")
  ToolRunner.run(configuration, new Tool,
    (classOf[JobA].getName :: runnerArgs.toList).toArray )

  log.info("Execute a [Scala] application")
  ScalaApp.main(null)

  log.info("Let's run an external system command")
  import sys.process._
  "ls -la" !

  log.info("Executing [Scalding] Job - B")
  ToolRunner.run(configuration, new Tool,
    (classOf[JobB].getName :: runnerArgs.toList).toArray )

}
