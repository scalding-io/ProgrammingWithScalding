package loganalysis

import com.twitter.scalding._

/**
 * --local @example  java -cp chapter4-0-jar-with-dependencies.jar -Xmx10G com.twitter.scalding.Tool loganalysis.step1 --input ~/log_file.tsv --local
 * @param args
 */
class step1(args: Args) extends Job(args) {

  val logSchema = List ('datetime, 'user, 'activity, 'data, 'session, 'location, 'response, 'device, 'error)

  Tsv( args("input"), logSchema)
   .read
   .filter('activity) { x:String => x=="login" }
   .write(Tsv("login_events"))
   .project('user, 'device)
   .write(Tsv("login_events_2_columns"))

}
