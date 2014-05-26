package tdd

import cascading.pipe.Pipe
import com.twitter.scalding.Dsl
import Dsl._
import tdd.ExampleSchema._

trait ExampleOperations {
  def pipe: Pipe

 /** In the context of Test-Driven-Development we will initially not provide an implementation
     Once tests fail - then we will proceed with proper implementations
  def logsAddDayColumn : Pipe = pipe
  def logsCountVisits : Pipe = pipe
  def logsJoinWithUsers(userData: Pipe) : Pipe = pipe
   */
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
