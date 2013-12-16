package loganalysis

import com.twitter.scalding._

class step1(args: Args) extends Job(args) {

  val logSchema = List ('datetime, 'user, 'activity, 'data, 'session, 'location, 'response, 'device, 'error)

  Tsv( args("input"), logSchema)
   .read
   .filter('activity) { x:String => x=="login" }
   .write(Tsv("login_events"))
   .project('user, 'device)
   .write(Tsv("login_events_2_columns"))

}
