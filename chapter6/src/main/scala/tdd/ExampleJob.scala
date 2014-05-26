package tdd

import com.twitter.scalding._
import com.twitter.scalding.Tsv

class ExampleJob(args: Args) extends Job(args) {
  import ExampleSchema._
  import tdd.ExampleWrapper._

/** The following is left as an exercise for the reader to implement more test cases **/
/*  val dailyVisits = Tsv(args("input"), LOG_SCHEMA).read
     .logsAddDayColumn
     .logsCountVisits
     .write(Tsv(args("output-daily-visits"), LOGS_DAILY_VISITS))
*/

  val visitsPerDay = Tsv(args("input"), LOG_SCHEMA).read
    .logsAddDayColumn
    .logsCountVisits
    .logsJoinWithUsers(Tsv(args("users"), USER_SCHEMA).read)
    .write(Tsv(args("output"),LOG_DAILY_WITH_ADDRESS))

}
