package functional

import org.scalatest.{Matchers, FlatSpec}
import com.twitter.scalding._
import scala.collection.mutable
import com.twitter.scalding.Tsv
import com.twitter.scalding.Osv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import tdd.{schemas, SampleJob}
import schemas._
@RunWith(classOf[JUnitRunner])
class SampleJobTest extends FlatSpec with Matchers with FieldConversions with TupleConversions {

  "A sample job" should "do the full transformation" in {

    JobTest(classOf[SampleJob].getName)
      .arg("eventsPath", "eventsPathFile")
      .arg("userInfoPath", "userInfoPathFile")
      .arg("outputPath", "outputPathFile")
      .source(Osv("eventsPathFile", INPUT_SCHEMA), List(("11/02/2013 10:22:11", 1000002l, "http://www.youtube.com")))
      .source(Osv("userInfoPathFile", USER_DATA_SCHEMA), List((1000002l, "stefano@email.com", "10 Downing St. London")))
      .sink(Tsv("outputPathFile", OUTPUT_SCHEMA)) {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
          buffer.toList shouldEqual List(("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l))
      }
      .run
  }
}
