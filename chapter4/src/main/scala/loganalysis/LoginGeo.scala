package loganalysis

import com.twitter.scalding._
import cascading.pipe.Pipe

class LoginGeo (args:Args) extends Job(args) {

  val schema = List ('datetime, 'user, 'activity, 'data,
    'session, 'location, 'response, 'device, 'error)

  def extractLoginGeolocationIntoJSONArray (input:Pipe) =
    input.filter('activity) { x:String => x=="login" }
      .project('location, 'device)
      .map ('location -> ('lat, 'lon)) {
        x:String => {
          val Array(lat, lon) = x.split(",")
          ("%4.2f" format lat.toFloat , "%4.2f" format lon.toFloat)
        }
      }
      .groupBy('lat, 'lon, 'device) { group => group.size('count).reducers(3) }
      .mapTo( ('lat, 'lon, 'device, 'count) -> 'json) { x:(String,String,String,String) =>
        val (lat,lon,device,count) = x
        s"""{"lat":$lat,"lon":$lon,"device":"$device",count:$count}"""
      }
      .groupAll { group => group.mkString('json, ",") }
      .map('json -> 'json) { x:String => "[" + x + "]" }

  val input = Tsv( args("input"), schema ).read
  val result = extractLoginGeolocationIntoJSONArray(input)
    .debug
    .write(Tsv( args("output") ))

}
