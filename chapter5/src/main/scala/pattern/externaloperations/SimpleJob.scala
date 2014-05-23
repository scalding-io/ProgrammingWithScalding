package externaloperations

import com.twitter.scalding._

class SimpleJob(args: Args) extends Job(args) {
  import LogsWrapper._
  import UserWrapper._
  import LogsSchemas._
  import UserSchemas._


  val dailyVisits = Tsv(args("logs"), LOG_SCHEMA).read
    .logsAddDayColumn
    .logsCountVisits
    .write(Tsv("logs-daily-visits/"+args("output"), LOGS_DAILY_VISITS))

  val visitsPerDay = Tsv(args("logs"), LOG_SCHEMA).read
    .logsAddDayColumn
    .logsCountVisits
    .logsJoinWithUsers(Tsv(args("users"), USER_SCHEMA).read)
    .write(Tsv("logs-visits-per-day/"+args("output"), LOGS_DAILY_VISITS))

 }