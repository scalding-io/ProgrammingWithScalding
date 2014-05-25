package unit

import org.scalatest.{Matchers, FlatSpec}
import scala.collection.mutable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalautils.Uniformity
import tdd.ExampleWrapper
import tdd.ExampleSchema
import cascading.pipe.Pipe

import ExampleSchema._
import ExampleWrapper._
import com.twitter.scalding.bdd.BddDsl

@RunWith(classOf[JUnitRunner])
class ExampleOperationsUnitTests extends FlatSpec with Matchers with BddDsl {

  "A sample job pipe transformation" should "add column with day of event" in {
    Given {
      List(("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com")) withSchema LOG_SCHEMA
    } When {
      pipe:Pipe => pipe.logsAddDayColumn
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        buffer.toList(0) should equal (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
    }
  }

  val sortedByDateAndIdAsc = new Uniformity[List[(String, Long, Long)]] {
    def normalized(list: List[(String, Long, Long)]) =
      list.sortWith { (left, right) => (left._1 < right._1) || ((left._1 == right._1) && (left._2 < left._2)) }

    def normalizedOrSame(b: Any): Any = b match {
      case list: List[(String, Long, Long)] => normalized(list)
      case _ => b
    }

    def normalizedCanHandle(b: Any): Boolean = b.isInstanceOf[List[(String, Long, Long)]]
  }

  it should "count user events per day" in {
    Given {
      List(
        ("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"),
        ("12/02/2013 10:22:11", 1000002l, "http://www.google.com", "2013/02/12"),
        ("11/02/2013 10:22:11", 1000002l, "http://www.amazon.com", "2013/02/11"),
        ("15/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000001l, "http://www.microsoft.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000003l, "http://www.youtube.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000001l, "http://www.google.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/15")
      ) withSchema ('datetime,'user,'url,'day)
    } When {
      pipe: Pipe => pipe.logsCountVisits
    } Then {
      buffer: mutable.Buffer[(String, Long, Long)] =>
        buffer.toList should equal (List(
          ("2013/02/11", 1000002l, 1l),
          ("2013/02/12", 1000002l, 2l),
          ("2013/02/15", 1000001l, 2l),
          ("2013/02/15", 1000002l, 2l),
          ("2013/02/15", 1000003l, 1l)
        )) (after being sortedByDateAndIdAsc)
    }
  }

  it should "add user info" in {
    Given {
      List(("2013/02/11", 1000002l, 1l)) withSchema LOGS_DAILY_VISITS
    } And {
      List((1000002l, "stefano@email.com", "10 Downing St. London")) withSchema USER_SCHEMA
    } When {
      (eventCount: Pipe, userData: Pipe) => eventCount.logsJoinWithUsers(userData)
    } Then {
      buffer: mutable.Buffer[(String, Long, Long, String, String)] =>
        buffer.toList should equal (List(("2013/02/11", 1000002l, 1l, "stefano@email.com", "10 Downing St. London")))
    }
  }
}
