package externaloperations

import cascading.pipe.Pipe
import com.twitter.scalding._
import com.twitter.scalding.Args
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.Job
import com.twitter.scalding.Osv
import com.twitter.scalding.RichPipe
import com.twitter.scalding.Tsv
import com.twitter.scalding.TupleConversions
import org.joda.time.format.DateTimeFormat
import com.twitter.scalding.Osv


package object sampleJobSchemas {
  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val WITH_DAY_SCHEMA = List('date, 'userid, 'url, 'day)
  val EVENT_COUNT_SCHEMA = List('day, 'userid, 'event_count)
  val OUTPUT_SCHEMA = List('day, 'userid, 'email, 'address, 'event_count)

  val USER_DATA_SCHEMA = List('userid, 'email, 'address)
}


trait SampleJobPipeOperations extends FieldConversions with TupleConversions {
  import sampleJobSchemas._
  import Dsl._

  val INPUT_DATE_PATTERN: String = "dd/MM/yyyy HH:mm:ss"

  def self: RichPipe

  /* Input schema: INPUT_SCHEMA
   * Output schema: WITH_DAY_SCHEMA */
  def addDayColumn : Pipe = self.map('date -> 'day) {
    date: String => DateTimeFormat.forPattern(INPUT_DATE_PATTERN)
      .parseDateTime(date).toString("yyyy/MM/dd");
  }

  /** Input schema: WITH_DAY_SCHEMA
    * Output schema: EVENT_COUNT_SCHEMA */
  def countUserEventsPerDay : Pipe =
    self.groupBy(('day, 'userid)) { _.size('event_count) }

  /** Joins with userData to add email and address
    *
    * Input schema: WITH_DAY_SCHEMA
    * User data schema: USER_DATA_SCHEMA
    * Output schema: OUTPUT_SCHEMA */
  def addUserInfo(userData: Pipe) : Pipe =
    self.joinWithLarger('userid -> 'userid, userData).project(OUTPUT_SCHEMA)
}

object SampleJobPipeOperationsWrappers {

  implicit def wrapPipe(self: Pipe): SampleJobPipeOperationsWrapper =
    new SampleJobPipeOperationsWrapper(new RichPipe(self))

  implicit class SampleJobPipeOperationsWrapper(val self: RichPipe) extends SampleJobPipeOperations
}

class SampleJob(args: Args) extends Job(args) {
  import SampleJobPipeOperationsWrappers._
  import sampleJobSchemas._

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addDayColumn
    .countUserEventsPerDay
    .addUserInfo(Osv(args("userInfoPath"), USER_DATA_SCHEMA).read)
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )
}

package object otherJobSchemas {
  val DAILY_EVENT_COUNT_SCHEMA = List('day, 'event_count)
}

object OtherJobPipeTransformationWrappers extends FieldConversions with TupleConversions {
  import Dsl._

  implicit def wrapPipe(self: Pipe): OtherJobPipeTransformationsWrapper =
    new OtherJobPipeTransformationsWrapper(new RichPipe(self))

  implicit class OtherJobPipeTransformationsWrapper(val self: RichPipe) extends SampleJobPipeOperations with Serializable {
    /** Input schema: WITH_DAY_SCHEMA
      * Output schema: DAILY_EVENT_COUNT_SCHEMA */
    def countEventsPerDay : Pipe =
      self.groupBy('day) { _.size('event_count) }
  }
}


class OtherJob(args: Args) extends Job(args) {
  import OtherJobPipeTransformationWrappers._
  import sampleJobSchemas._
  import otherJobSchemas._

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addDayColumn
    .countEventsPerDay
    .write( Tsv(args("outputPath"), DAILY_EVENT_COUNT_SCHEMA) )
}