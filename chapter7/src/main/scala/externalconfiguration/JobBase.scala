package externalconfiguration

import com.twitter.scalding._
import com.typesafe.config.ConfigFactory

class JobBase(args: Args) extends Job(args) {

  val appConfig = ConfigFactory.parseFile(new java.io.File(getValue("cluster-config")))

  def getValue(key: String): String = {
    args.m.get(key) match {
      case Some(v) => v.head
      case None => sys.error(s"Argument [${key}] - NOT FOUND")
    }
  }

}