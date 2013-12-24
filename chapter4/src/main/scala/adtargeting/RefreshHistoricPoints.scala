package adtargeting

import com.twitter.scalding._

/**
 * This Scalding job will read today's daily points
 * and yesterday's historic data to update historic data
 * for today
 *
 * @author Antonios Chalkiopoulos - Antwnis@gmail.com
 *
 * @example ...
 */
class RefreshHistoricPoints(args:Args) extends Job(args) {

  val input_daily_points = args.getOrElse("input_dailypoits", "/data/dailyadpoints/2014/01/01/")
  val input_historic     = args.getOrElse("input_historic",   "/data/histpoints/2013/12/31/")
  val output_historic    = args.getOrElse("output_historic",  "/data/histpoints/2014/01/01/")

  val pointsSchema = List('user,'category,'subcategory,'points)

  val dailyInput = List (
    ("1","sports","football",10),
    ("1","sports","rugby", 2))

  val historyInput = List (
    ("1","sports","football",200),
    ("1","sports","rugby", 18),
    ("1","sports","F1", 15),
    ("1","travel","budget",10))

  val dailyPipe =IterableSource[(String,String, String, Int)](dailyInput, pointsSchema)
    .read

  val historyPipe =IterableSource[(String,String, String, Int)](historyInput, pointsSchema)
    .read

  val updatedHistory =
    (dailyPipe ++ historyPipe)
    .groupBy('user,'category,'subcategory)
      { group => group.sum[Int]('points) }
    .debug
    .write(Tsv("output-ad-targeting-updatedHistory"))



}
