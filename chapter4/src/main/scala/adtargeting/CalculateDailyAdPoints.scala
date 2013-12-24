package adtargeting

import com.twitter.scalding._
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone

class CalculateDailyAdPoints (args: Args) extends Job(args) {

    val schema = List('user,'datetime,'activity,'data)

    val input = List (
      ("1","2014-01-01 09:00:00","login",""),
      ("1","2014-01-01 09:00:05","readArticle","sports/rugby/12"),
      ("1","2014-01-01 09:00:20","readArticle","sports/rugby/7"),
      ("1","2014-01-01 09:01:00","readArticle","sports/football/4"),
      ("1","2014-01-01 09:02:30","readArticle","sports/football/11"),
      ("1","2014-01-01 09:03:50","streamVideo","sports/football/11"),
      ("1","2014-01-01 09:05:00","readArticle","sports/football/2"),
      ("1","2014-01-01 11:05:00","readArticle","sports/football/3"))

    /**
     * Function to calculate the Unix time-stamp 'epoch' from a DateTime
     * with the format "yyyy-MM-dd HH:mm:ss"
     */
    val timezone = DateTimeZone.forID("Europe/London")
    def calculateEpoch(dateTime: String, dateFormat: String = "yyyy-MM-dd HH:mm:ss") = {
      val fmt = DateTimeFormat.forPattern(dateFormat).withZone(timezone)
      fmt.parseDateTime(dateTime).getMillis / 1000
    }

    val dailyPipe =IterableSource[(String,String, String, String)](input, schema)
      .read

      // Calculate duration using buffers in scanLeft
      // Create a helper symbol first
      .insert('temp, 0L)
      // Convert to epoch
      .map('datetime -> 'epoch)
        { x: String => calculateEpoch(x) }
      // Group and sort by epoch in reverse, so that most recent event comes first
      .groupBy('user) { group =>
        group.sortBy('epoch).reverse
          .scanLeft(('epoch, 'temp) -> ('bufferedEpoch, 'duration))((0L, 0L)) {
          (bufferedLine: (Long, Long), currentLine: (Long, Long)) =>
            val bufferedEpoch = bufferedLine._1
            //val temp = firstLine._2
            val epoch = currentLine._1
            val duration = bufferedEpoch - epoch
            // return the current epoch to be buffered for next calcualtion
            // and the calculated duration for this line
            (epoch, duration)
        }
      }
      .write(Tsv("output-adtargeting-scanLeft-result"))

      // scanLeft is initialised with (0L,0L) so first subtraction
      // will result into a negative number!
      .filter('duration) { x: Long => x > 0 }
      .discard('bufferedEpoch, 'epoch, 'temp)
      .write(Tsv("output-adtargeting-scanLeft-cleaned"))

      // Calculate points per line
      .map(('activity , 'duration) -> 'points)
        { x:(String,Int) =>
        val action = x._1
        val duration = x._2
        action match {
          case "streamVideo" => 3
          case "readArticle" => if ((duration>=60) && (duration<600) ) 3 else 1
          case _ => 0
        }
      }
      .write(Tsv("output-adtargeting-points-per-line"))

      // split to category/subategory and calculate aggregations
      .filter('points) { x: Int => x > 0 }
      .map('data -> ('category, 'subcategory)) { x: String =>
        val firstSlash = x.indexOf("/")
        val secondSlash = x.indexOf("/", firstSlash+1)
        if (firstSlash != -1 || secondSlash != -1)
          (x.substring(0,firstSlash),x.substring(firstSlash+1, secondSlash))
        else
          ("","")
      }

      // calculate final daily points per user and per category-subcategory
      .groupBy('user,'category,'subcategory)
        { group => group.sum[Int]('points) }

      .debug
      .write(Tsv("output-adtargeting-dailyAdPoints"))

      // Result will look like
      // ['1', 'sports', 'football', '10']
      // ['1', 'sports', 'rugby', '2']

  val historicData = List (
    ("1","sports","football",210),
    ("1","sports","rugby", 18),
    ("1","sports","F1",15),
    ("1","travel","family",10))

  val historySchema = ('user,'category,'subcategory,'points)

  val historyPipe = IterableSource[(String,String, String, Int)](historicData, historySchema)
    .read

  val yesterdayData = List(("1","culture","theater",12))
  val yesterdayPipe = IterableSource[(String,String, String, Int)](yesterdayData, historySchema)
    .read

  // Today takes 40%
  val pipe1 = dailyPipe.map('points -> 'points)
  { x:Long => x*0.4 }
  // Yesterday takes 30% of the value
  val pipe3 = yesterdayPipe.map('points -> 'points)
  { x:Long => x*0.3 }
  // History takes 30% of the value
  val pipe2 = historyPipe.map('points -> 'points)
   { x:Long => x*0.3 }

  val topUserPreferences = (pipe1 ++ pipe2 ++ pipe3)
  .groupBy('user,'category, 'subcategory)
    { group => group.sum[Long]('points) }
  .groupBy('user)
    { group => group.sortBy('points).reverse.take(1) }
  .debug
  .write(Tsv("aaa"))

  val adsData = List(("sports","football","ad123"))
  val adsSchema = List('category, 'subcategory, 'adID)
  val adsPipe = IterableSource[(String,String, String)](adsData, adsSchema)
    .read

  topUserPreferences.joinWithTiny(('category, 'subcategory)->('category, 'subcategory), adsPipe )
    .write(Tsv("suggested_ads"))





}
