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
 */
class CreateMockLogData(args: Args) extends Job(args) {

  // This is the schema of the data
  val logSchema = List ('datetime, 'user, 'activity, 'data, 'session, 'location, 'response, 'device, 'error, 'server)

  /**
   * Let's generate file log_file.tsv
   */
  val numberOfLines = 100000
  val aList = (1 to numberOfLines).toList
  val rawLogsPipe =
    IterableSource[Int](aList, 'numbers)
      .mapTo('datetime, 'user, 'activity, 'data, 'device)
        // We use the method 'generateFakeData'
        { x:Int => generateFakeData(x) }
      .write(Tsv("log_file.tsv"))

  /**
   * Following block of code is used to generate a random log line
   */
  val random = new scala.util.Random
  def generateFakeData( x:Int ) = {
    // Use randomness to generate a date-time from 1st Jan 2014 00:00:00 .. 1st Jan 2014 23:59:59
    val datetime = new DateTime(1388534400L*1000 + random.nextInt(86400*1000)).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ") // Let's assume the ISO8601 is used
    // Assume that we will generate logs for 100 users
    val user = x % 100
    // Assume that 3 types of events are logged: login, readArticle and streamContent
    val activity = List("login","stream","readArticle").get(random.nextInt(3))
    // Column 'data holds information regarding the specific activity
    val data = generateData(activity)
    // Simulate a a random location, assuming that 60% of our users live in New York, 35% live in London
    val location = generateRandomLocation(x)

    val device_type = List("ipad","iphone","xbox","ps3","pc","android").get(random.nextInt(6))
    // Return a fake log-line as a tuple with the expected schema
    // Unlike Java, in Scala we don't need to use the 'return' keyword,
    // the last line of a method is actually our return statement
    (datetime, "user-"+user , activity, location, device_type )
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
      "article/" + contentTypes.get(random.nextInt(25)) + "/" + random.nextInt(10)
      "/articles/sports/"
    } else {
      ""
    }
  }



}

