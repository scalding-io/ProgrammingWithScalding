package generatedata

import com.twitter.scalding._
import org.joda.time.DateTime

/**
 * This Scalding job can be used to generate a 5GByte file
 * that contains mock data, to be used for log file analysis
 * and ad targeting applications presented in chapter4
 * of 'Programming Map-Reduce in Scalding'
 *
 * @author Antonios Chalkiopoulos - Antwnis@gmail.com
 *
 */
class CreateMockLogData(args: Args) extends Job(args) {

  // This is the schema of the data
  val logSchema = List ('datetime, 'user, 'activity, 'data, 'session, 'location, 'response, 'device, 'error)

  /**
   * Let's generate file log_file.tsv
   *
   * By default this will generate 0.5 GB data -> 3.5 Million lines - and requires ~ 1 GB Ram
   * To generate 5 GByte -> 35.000.000 log lines - you will need a bigger Heap Space
   */
  val numberOfLines = args.optional("num.of.lines").getOrElse("3500000").toInt
  val aList = (1 to numberOfLines).toList
  val rawLogsPipe =
    IterableSource[Int](aList, 'numbers)
      .mapTo(logSchema)
        // We use the method 'generateFakeData'
        { x:Int => generateFakeData(x) }
      .write(Tsv("log_file.tsv"))

  /**
   * Following block of code is used to generate a random log line
   * ('datetime, 'user, 'activity, 'data, 'session, 'location, 'response, 'device, 'error)
   */
  val random = new scala.util.Random
  def generateFakeData( x:Int ) = {
    // Use randomness to generate a date-time from 1st Jan 2014 00:00:00 .. 1st Jan 2014 23:59:59
    val datetime = new DateTime(1388534400L*1000 + random.nextInt(86400*1000)).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ") // Let's assume the ISO8601 is used
    // Assume that we will generate logs for 100 users
    val user = x % 100
    // Assume that 3 types of events are logged: login (5%), readArticle (80%) and streamVideo (15%)
    val r = random.nextInt(100)
    val activity = if (r < 5) "login" else if (r<85) "readArticle" else "streamVideo"
    // Column 'data holds information regarding the specific activity
    val data = generateData(activity)
    val session = java.util.UUID.randomUUID.toString
    // Simulate a a random location, assuming that 60% of our users live in New York, 35% live in London
    val location = generateRandomLocation(x)
    val response = (random.nextInt(250) + 20) + " msec"
    val device = List("ipad","iphone","xbox","ps3","pc","android").get(x % 6)
    // 1 in 100.000 requests results into some error message
    val r2 = random.nextInt(100000)
    val error = if (r2==0) "Error in API call" else ""
    // Return a fake log-line as a tuple with the expected schema
    // Unlike Java, in Scala we don't need to use the 'return' keyword,
    // the last line of a method is actually our return statement
    (datetime, user, activity, data, session, location, response, device, error)
  }

  /**
   * In this method we will assume for the sake of the simulation that
   * 60% of our users come from New York , 35% from London and 5% from Tokyo
   *
   */
  def generateRandomLocation( x:Int ) = {
    // Assume each city has a radius of 0.135 decimal degrees
    val latitude_randomness  = (random.nextInt(270) - 135 ) /1000D
    val longitude_randomness = (random.nextInt(270) - 135 ) /1000D
    // For NY assume center is lat=40.70 & lng=-74
    if ( x <= 60) {
      (40.70D + latitude_randomness) + "," + (-74D + longitude_randomness)
    }
    // For London assume center is lat=51.5 & lng=-0.1
    else if (x <= 95) {
      (51.5D + latitude_randomness) + "," + (-0.1D + longitude_randomness)
    }
    // For Tokyo assume center is lat=35.7 & lng=139.6
    else {
      (35.7D + latitude_randomness) + "," + (139.6D + longitude_randomness)
    }
  }

  // Let's assume that 5 main categories of content with 5 subcategories exist
  val contentTypes = ("sport/football", "sport/rugby"  , "sport/tennis" , "sport/F1"     ,"sport/cycling",
                      "tech/games"    , "tech/mobile"  , "tech/gadgets" , "tech/apps"    ,"tech/internet",
                      "culture/books" , "culture/film" , "culture/music", "culture/art"  ,"culture/theater",
                      "travel/hotels" , "travel/skiing", "travel/family", "travel/budget","travel/breaks")
  def generateData( activity:String ) = {
    if (activity == "login") {
      "Successful login - User subscription: 1234567 valid for 20 days"
    } else if (activity == "readArticle") {
      // Let's assume that 10 articles exist for each subcategory
      "article/" + contentTypes.get(random.nextInt(20)) + "/" + random.nextInt(10)
    } else {
      // Let's assume that 10 videos exist for each subcategory
      "video/" + contentTypes.get(random.nextInt(20)) + "/" + random.nextInt(10)
    }
  }

}
