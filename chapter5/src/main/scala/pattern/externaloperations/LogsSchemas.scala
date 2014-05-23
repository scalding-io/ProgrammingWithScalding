package externaloperations

package object LogsSchemas {

  // i.e. hdfs:///logs/raw/YYYY/MM/DD/
  val LOG_SCHEMA = List('datetime, 'user, 'url)

  // i.e. hdfs:///logs/daily-visits/YYYY/MM/DD/
  val LOGS_DAILY_VISITS = ('day, 'user, 'visits)

  // i.e. hdfs:///logs/daily-visits-with-address/YYYY/MM/DD/
  val LOG_DAILY_WITH_ADDRESS = List('day, 'user, 'email, 'address, 'visits)

}