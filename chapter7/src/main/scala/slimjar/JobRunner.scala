package slimjar

import com.twitter.scalding.Tool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop
import org.slf4j.LoggerFactory

object JobRunner {

  def main(args : Array[String]) {

    val log = LoggerFactory.getLogger(this.getClass.getName)

    val conf: Configuration = new Configuration

    if (args.contains("--heapInc")) {
      log.info("Setting JVM Memory/Heap Size for every child mapper and reducer")
      val jvmOpts = "-Xmx4096m -XX:+PrintGCDetails -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=50"
      log.info("**** JVM Options : " + jvmOpts )
      conf.set("mapred.child.java.opts", jvmOpts);
    }

    AppConfig.jobConfig = conf

    hadoop.util.ToolRunner.run(conf, new Tool, args);
  }
}