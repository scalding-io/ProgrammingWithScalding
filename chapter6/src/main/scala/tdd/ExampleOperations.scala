package tdd

import cascading.pipe.Pipe
import com.twitter.scalding.{RichPipe, Dsl}
import Dsl._
import com.pragmasoft.scaldingunit.PipeOperationsConversions

//import com.twitter.scalding.bdd.PipeOperationsConversions

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

  def logsJoinWithUsers(userData: Pipe) : Pipe =
    pipe.joinWithLarger('user -> 'user, userData)
}

object ExampleOperations {

  implicit class ExampleWrapper(val pipe: Pipe) extends ExampleOperations with Serializable
  implicit def wrapperFromRichPipe(rp: RichPipe): ExampleWrapper = new ExampleWrapper(rp.pipe)

}