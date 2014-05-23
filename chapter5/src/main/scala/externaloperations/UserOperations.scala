package externaloperations

import cascading.pipe.Pipe
import com.twitter.scalding._

trait UserOperations extends FieldConversions with TupleConversions {

  def self: RichPipe

  def usersCountDailyVisits : Pipe =
      self.groupBy('day) { _.size('visits) }

}