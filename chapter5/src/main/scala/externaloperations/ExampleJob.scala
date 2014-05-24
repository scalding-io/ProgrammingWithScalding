package externaloperations

import com.twitter.scalding._

class ExampleJob(args: Args) extends Job(args) {
  import LogsWrapper._
  import UserWrapper._
  import LogsSchemas._
  import UserSchemas._

  val dailyVisits = Tsv(args("input"), LOG_SCHEMA).read
    .logsAddDayColumn
    .logsCountVisits
    .write(Tsv(args("output")+"/logs-daily-visits.tsv", LOGS_DAILY_VISITS))

  val visitsPerDay = Tsv(args("input"), LOG_SCHEMA).read
    .logsAddDayColumn
    .logsCountVisits
    .logsJoinWithUsers(Tsv(args("users"), USER_SCHEMA).read)
    .write(Tsv(args("output")+"/logs-visits-per-day.tsv",LOG_DAILY_WITH_ADDRESS))

 }