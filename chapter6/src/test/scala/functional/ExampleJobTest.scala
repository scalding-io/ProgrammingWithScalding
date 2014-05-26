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

  "FUNCTIONAL2 TEST - A sample job" should "do the full transformation" in {

    val logs = List(
      ("01/07/2014 10:22:11", 1000002L, "http://youtube.com"),
      ("01/07/2014 10:22:11", 1000003L, "http://twitter.com"),
      ("01/07/2014 10:22:11", 1000002L, "http://google.com"),
      ("01/07/2014 10:22:11", 1000002L, "http://facebook.com")
    )
    val users = List(
      (1000002L, "stefano@email.com", "10 Downing St. London"),
      (1000003L, "antonios@email.com", "1 Kingdom St. London")
    )
    val expectedOutput = List(
      ("2014/07/01", 1000002L, 3L, "stefano@email.com","10 Downing St. London"),
      ("2014/07/01", 1000003L, 1L, "antonios@email.com", "1 Kingdom St. London")
    )

    JobTest(classOf[ExampleJob].getName)
      .arg("input", "input-logs")
      .arg("users", "users-logs")
      .arg("output1", "output-logs-daily-visits")
      .arg("output2", "output-logs-daily-with-address")
      .source(Tsv("input-logs", LOG_SCHEMA), logs)
      .source(Tsv("users-logs", USER_SCHEMA), users)
      .sink(Tsv("output-logs-daily-with-address", LOG_DAILY_WITH_ADDRESS)) {
        buffer: mutable.Buffer[(String, Long, Long, String, String)] =>
          buffer.toList should equal(expectedOutput)
      }
      .run // or .runHadoop
  }
}
