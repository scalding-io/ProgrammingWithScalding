package functional

import com.twitter.scalding._
import org.scalatest.{Matchers, FlatSpec}
import scala.collection.mutable
import tdd.{ExampleSchema, ExampleJob}

//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//@RunWith(classOf[JUnitRunner])

class ExampleJobTest extends FlatSpec with Matchers with FieldConversions with TupleConversions {

  import ExampleSchema._

  "FUNCTIONAL - A sample job" should "do the full transformation" in {

    JobTest(classOf[ExampleJob].getName)
      .arg("input", "input-logs")
      .arg("users", "users-logs")
      .arg("output1", "output-logs-daily-visits")
      .arg("output2", "output-logs-daily-with-address")
      .source(Tsv("input-logs", LOG_SCHEMA), List(("11/02/2013 10:22:11", 1000002l, "http://www.youtube.com")))
      .source(Tsv("users-logs", USER_SCHEMA), List((1000002l, "stefano@email.com", "10 Downing St. London")))
      .sink(Tsv("output-logs-daily-visits", LOGS_DAILY_VISITS)) {
        buffer: mutable.Buffer[(String, Long, Long)] =>
          buffer.toList shouldEqual List(("2013/02/11", 1000002l, 1l))
      }
      .sink(Tsv("output-logs-daily-with-address", LOG_DAILY_WITH_ADDRESS)) {
      buffer: mutable.Buffer[(String, Long, Long, String, String)] =>
          buffer.toList shouldEqual List(("2013/02/11", 1000002l, 1l, "stefano@email.com", "10 Downing St. London"))
      }
      .run
  }
}
