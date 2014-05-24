package functional

import com.twitter.scalding._
import org.scalatest.{Matchers, FlatSpec}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable
import tdd.{ExampleSchema, ExampleJob}

@RunWith(classOf[JUnitRunner])
class ExampleJobTest extends FlatSpec with Matchers with FieldConversions with TupleConversions {

  import ExampleSchema._

  "A sample job" should "do the full transformation" in {

    JobTest(classOf[ExampleJob].getName)
      .arg("input", "input-logs")
      .arg("users", "users-logs")
      .arg("output", "output-path")
      .source(Tsv("input-logs", LOG_SCHEMA), List(("11/02/2013 10:22:11", 1000002l, "http://www.youtube.com")))
      .source(Tsv("users-logs", USER_SCHEMA), List((1000002l, "stefano@email.com", "10 Downing St. London")))
      .sink(Tsv("output-path", LOG_DAILY_WITH_ADDRESS)) {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
          buffer.toList shouldEqual List(("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l))
      }
      .run
  }
}
