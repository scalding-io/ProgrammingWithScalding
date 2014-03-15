package elasticsearch

import org.elasticsearch.hadoop.cascading._
import com.twitter.scalding._

import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields

/**
 * https://github.com/rore/SpyGlass/blob/master/src/main/scala/parallelai/spyglass/hbase/HBaseSource.scala
 * https://github.com/twitter/scalding/blob/02a7affe768850cb39e959413020d52ffa3987e9/scalding-core/src/main/scala/com/twitter/scalding/FileSource.scala
 * https://github.com/twitter/scalding/blob/develop/scalding-jdbc/src/main/scala/com/twitter/scalding/jdbc/JDBCSource.scala
 */
case class ElasticSearchSource(
   es_host :String="localhost",
   es_port :Int   = 9200,
   es_resource:String="scalding_index/type",
   es_fields : Fields = Fields.ALL)
  extends Source {


  def createTap: Tap[_, _, _] =
    new EsTap(es_host, es_port, es_resource,"",es_fields)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Local(_) => {
        createTap
      }
//      case hdfsMode@Hdfs(_, _) => readOrWrite match {      }
    }
  }

}

//  val sinkMode: SinkMode = SinkMode.REPLACE


//      case hdfsMode@Hdfs(_, _) => readOrWrite match {
//        case Read => createHdfsReadTap(hdfsMode)
//        //case Write => EsTap() // CastHfsTap(new Hfs(hdfsScheme, hdfsWritePath, sinkMode))
//      }
//      case _ => {
//        new Tap[_,_,_] //println("aaaa")
//      }

//   val hbt = new EsTap() //"quorumNames", "tableName", "hBaseScheme", "SinkMode.KEEP"
//   hbt.asInstanceOf[Tap[_,_,_]]
//    }
//
//  }

//  protected def createHdfsReadTap(hdfsMode : Hdfs) : Tap[JobConf, _, _] = {
//    val taps : List[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]]] =
//      goodHdfsPaths(hdfsMode).toList.map { path => CastHfsTap(new Hfs(hdfsScheme, path, sinkMode)) }
//    taps.size match {
//      case 0 => {
//        // This case is going to result in an error, but we don't want to throw until
//        // validateTaps, so we just put a dummy path to return something so the
//        // Job constructor does not fail.
//        CastHfsTap(new Hfs(hdfsScheme, hdfsPaths.head, sinkMode))
//      }
//      case 1 => taps.head
//  //    case _ => new ScaldingMultiSourceTap(taps)
//    }
//  }

//}