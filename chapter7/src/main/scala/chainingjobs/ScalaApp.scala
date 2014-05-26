package chainingjobs

/**
 * In this application add any application logic, like calling external web services to fetch data
 * , trigger other processes, execute arbitrary Scala/Java code etc.
 */
object ScalaApp extends App {
  println("Application logic")
  println("..sleeping 5 seconds..")

  Thread sleep 5000

  println("Now proceeding with [Scalding] job")
}