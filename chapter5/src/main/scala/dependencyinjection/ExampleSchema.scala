package dependencyinjection

package object ExampleSchema {

  // i.e. hdfs:///logs/raw/YYYY/MM/DD/
  val LOG_SCHEMA = List('datetime, 'user, 'url)
  //val INPUT_SCHEMA = List('date, 'user, 'url)

  // i.e. hdfs:///logs/by-date-with-address/YYYY/MM/DD/
  val OUTPUT_SCHEMA = List('datetime, 'user, 'url, 'email, 'address)

}