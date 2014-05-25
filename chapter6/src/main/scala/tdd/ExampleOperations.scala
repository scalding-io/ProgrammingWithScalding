package tdd

import cascading.pipe.Pipe
import com.twitter.scalding.Dsl
import Dsl._
import tdd.ExampleSchema._

trait ExampleOperations {
  def pipe: Pipe

  val fmt = org.joda.time.format.DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")

  def logsAddDayColumn : Pipe = {
    pipe
      .map('datetime -> 'day) {
        date: String => fmt.parseDateTime(date).toString("yyyy/MM/dd")
      }
  }

  def logsCountVisits : Pipe = pipe
    .groupBy(('day, 'user)) { _.size('visits) }

  def logsJoinWithUsers(userData: Pipe) : Pipe = pipe
    .joinWithLarger('user -> 'user, userData)
    .project(LOG_DAILY_WITH_ADDRESS)

}
