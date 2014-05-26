package unit

import org.scalatest.{Matchers, FlatSpec}
import scala.collection.mutable

import org.scalautils.Uniformity
import tdd.ExampleWrapper
import tdd.ExampleSchema
import cascading.pipe.Pipe

import com.twitter.scalding.bdd.BddDsl

//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//@RunWith(classOf[JUnitRunner])

class ExampleOperationsUnitTests extends FlatSpec with Matchers with BddDsl {
  import ExampleSchema._
  import ExampleWrapper._

  "Unit-Test: A sample job pipe transformation" should "add column with day of event" in {
    Given {
      List(("12/07/2014 10:22:11", 1000002L, "http://www.youtube.com")) withSchema LOG_SCHEMA
    } When {
      pipe:Pipe => pipe.logsAddDayColumn
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        buffer.toList(0) should equal (("12/07/2014 10:22:11", 1000002L, "http://www.youtube.com", "2014/07/12"))
    }
  }

  it should "add user info" in {
    Given {
      List(("2014/07/01", 1000002L, 1L)) withSchema LOGS_DAILY_VISITS
    } And {
      List((1000002L, "stefano@email.com", "10 Downing St. London")) withSchema USER_SCHEMA
    } When {
      (logs: Pipe, users: Pipe) => logs.logsJoinWithUsers(users)
    } Then {
      buffer: mutable.Buffer[(String, Long, Long, String, String)] =>
        buffer should equal (List(("2014/07/01", 1000002L, 1L, "stefano@email.com", "10 Downing St. London")))
    }
  }

  it should "count visits per day" in {
    Given {
      List(
        ("2014/07/01", 1000002L, "http://youtube.com"),
        ("2014/07/01", 1000003L, "http://twitter.com"),
        ("2014/07/01", 1000002L, "http://google.com"),
        ("2014/07/01", 1000002L, "http://facebook.com")
      ) withSchema ('day, 'user, 'url)

    } When {
      pipe: Pipe => pipe.logsCountVisits
    } Then {
      buffer: mutable.Buffer[(String, Long, Long)] =>
        buffer.toSet should equal (Set(
          ("2014/07/01", 1000002L, 3L),
          ("2014/07/01", 1000003L, 1L)
        ))
        // Or
        // buffer.toList should equal (List(
        //  ("2014/07/01", 1000002L, 3L),
        //  ("2014/07/01", 1000003L, 1L)
        // )) (after being sortedByDateAndIdAsc)
    }
  }

  /**
   * Helper method to sort our mock data
   */
  val sortedByDateAndIdAsc = new Uniformity[List[(String, Long, Long)]] {
    def normalized(list: List[(String, Long, Long)]) =
      list.sortWith { (left, right) => (left._1 < right._1) || ((left._1 == right._1) && (left._2 < left._2)) }
    def normalizedOrSame(b: Any): Any = b match {
      case list: List[(String, Long, Long)] => normalized(list)
      case _ => b
    }
    def normalizedCanHandle(b: Any): Boolean = b.isInstanceOf[List[(String, Long, Long)]]
  }

}
