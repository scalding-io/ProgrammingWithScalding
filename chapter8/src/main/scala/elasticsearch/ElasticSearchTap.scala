package elasticsearch

import com.twitter.scalding._
import cascading.tap.Tap
import cascading.tuple.Fields
import org.elasticsearch.hadoop.cascading._

/**
 * A simple tap for elastic-search
 */
case class ElasticSearchTap(
   esHost :String,
   esPort :Int   ,
   esResource:String,
   esQuery:String,
   esFields : Fields)
  extends Source {

  def createEsTap: Tap[_, _, _] =
    new EsTap(esHost, esPort, esResource, esQuery, esFields)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Local(_) | Hdfs(_, _) => createEsTap
    }
  }
}

/**
 * Other taps:
 *
 * https://github.com/rore/SpyGlass/blob/master/src/main/scala/parallelai/spyglass/hbase/HBaseSource.scala
 * https://github.com/twitter/scalding/blob/02a7affe768850cb39e959413020d52ffa3987e9/scalding-core/src/main/scala/com/twitter/scalding/FileSource.scala
 * https://github.com/twitter/scalding/blob/develop/scalding-jdbc/src/main/scala/com/twitter/scalding/jdbc/JDBCSource.scala
 */
