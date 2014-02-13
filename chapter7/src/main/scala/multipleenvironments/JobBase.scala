package multipleenvironments

import com.twitter.scalding.Job
import com.twitter.scalding.Args
import com.typesafe.config.ConfigFactory

class JobBase(args: Args) extends Job(args) {

  val appConfig = ConfigFactory.parseFile(new java.io.File(getString("cluster-config")))

  def getString(key: String): String = {
    args.m.get(key) match {
      case Some(v) => v.head
      case None => sys.error(s"Argument [${key}] - NOT FOUND")
    }
  }

}