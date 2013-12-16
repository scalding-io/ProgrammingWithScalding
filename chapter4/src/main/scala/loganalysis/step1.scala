package loganalysis

import com.twitter.scalding._

class step1(args: Args) extends Job(args) {

  val MPOD_INPUT_SCHEMA  = List('activity_time, 'user_id, 'activity_type, 'channel, 'activity_data, 'error_note, 'site_id,'source, 'session_id, 'response_time, 'device_type, 'device_id)
  Tsv( args("input"), MPOD_INPUT_SCHEMA)
   .read
   .filter('activity) { x:String => x=="login" }
   .project('user_id, 'device)
    //x:String => json"{ x: $x} "}
   .write(Tsv( args("output")))

}
