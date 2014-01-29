package tdd

import cascading.pipe.Pipe
import com.pragmasoft.scaldingunit.PipeOperations
import com.twitter.scalding._
import com.github.nscala_time.time.Imports._
import com.twitter.scalding.Osv
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import SampleJobPipeTransformations.SampleJobPipeTransformationsWrapper
import scala.language.implicitConversions

package object schemas {
  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val WITH_DAY_SCHEMA = List('date, 'userid, 'url, 'day)
  val EVENT_COUNT_SCHEMA = List('day, 'userid, 'event_count)
  val OUTPUT_SCHEMA = List('day, 'userid, 'email, 'address, 'event_count)

  val USER_DATA_SCHEMA = List('userid, 'email, 'address)
}

trait SampleJobPipeTransformations extends PipeOperations {
  import schemas._

  val INPUT_DATE_PATTERN: String = "dd/MM/yyyy HH:mm:ss"

  def self: RichPipe

  /**
   * Input schema: INPUT_SCHEMA
   * Output schema: WITH_DAY_SCHEMA
   * @return
   */
  def addDayColumn : Pipe = self.map('date -> 'day) {
    date: String => DateTimeFormat.forPattern(INPUT_DATE_PATTERN).parseDateTime(date).toString("yyyy/MM/dd");
  }

  /**
   * Input schema: WITH_DAY_SCHEMA
   * Output schema: EVENT_COUNT_SCHEMA
   * @return
   */
  def countUserEventsPerDay : Pipe = self.groupBy(('day, 'userid)) { _.size('event_count) }

  /**
   * Joins with userData to add email and address
   *
   * Input schema: WITH_DAY_SCHEMA
   * User data schema: USER_DATA_SCHEMA
   * Output schema: OUTPUT_SCHEMA
   */
  def addUserInfo(userData: Pipe) : Pipe = self.joinWithLarger('userid -> 'userid, userData).project(OUTPUT_SCHEMA)
}

object SampleJobPipeTransformations  {
  implicit def wrapPipe(pipe: Pipe): SampleJobPipeTransformationsWrapper = new SampleJobPipeTransformationsWrapper(new RichPipe(pipe))
  implicit class SampleJobPipeTransformationsWrapper(val self: RichPipe) extends SampleJobPipeTransformations with Serializable
}

import SampleJobPipeTransformations._
import schemas._

class SampleJob(args: Args) extends Job(args) {

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addDayColumn
    .countUserEventsPerDay
    .addUserInfo(Osv(args("userInfoPath"), USER_DATA_SCHEMA).read)
    .write(Tsv(args("outputPath"), OUTPUT_SCHEMA))
}
