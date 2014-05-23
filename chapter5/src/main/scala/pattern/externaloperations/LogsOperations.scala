package externaloperations

import cascading.pipe.Pipe
import com.twitter.scalding._

trait LogsOperations extends FieldConversions with TupleConversions {

  def self: RichPipe

  val fmt = org.joda.time.format.DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")

  def logsAddDayColumn : Pipe = self
    .map('datetime -> 'day) {
      date: String => fmt.parseDateTime(date).toString("yyyy/MM/dd")
    }

  def logsCountVisits : Pipe = self
    .groupBy(('day, 'user)) { _.size('visits) }

  def logsJoinWithUsers(userData: Pipe) : Pipe =
    self.joinWithLarger('user -> 'user, userData)
}
