package adtargeting

import com.twitter.scalding.{Tsv, IterableSource, Job, Args}
import org.joda.time.format.DateTimeFormat

class WindowedCalculations (args: Args) extends Job(args) {

   val data = List(
     ("2014-01-01 13:25:36.172", "user-653", "readArticle", "article1"),
     ("2014-01-01 13:28:12.271", "user-653", "readArticle", "article2"),
     ("2014-01-01 17:28:58.261", "user-653", "readArticle", "article3"),
     ("2014-01-01 17:31:31.125", "user-653", "readArticle", "article4"))

  val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
  val timeout = 300 // 300 seconds = 5 minutes


  val rawLogsPipe =
    IterableSource[(String, String,String,String)](data, ('date, 'user_id, 'action, 'action_data))
    .read
    .map('date -> 'epoch) { x:String => (fmt.parseDateTime(x)).getMillis/1000 }
    // Create a helper symbol first
    .insert('temp, 0L)
    // Group by user and sort by epoch in reverse, so that most recent event comes first
    .groupBy('user_id) { group =>
      group
        .sortBy('epoch).reverse
        .scanLeft(('epoch, 'temp) -> ('originalEpoch, 'duration))((0L, 0L)) {
           (firstLine: (Long, Long), secondLine: (Long, Long)) =>
           var delta = firstLine._1 - secondLine._1
           // scanLeft is initialised with (0L,0L) so first subtraction
           // will result into a negative number!
           if (delta < 0L) delta = -delta
           (secondLine._1, delta)
        }
    }
    .debug
    .write(Tsv("output-Windowed"))

}
