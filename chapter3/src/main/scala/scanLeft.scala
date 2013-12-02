import com.twitter.scalding.{IterableSource, Job, Args, TextLine}

// scanLeft example
class scanLeft(args: Args) extends Job(args) {

  // --- A simple ranking job
  val sampleInput1 = List(
    ("male", "165.2"),
    ("female", "172.2"),
    ("male", "184.1"),
    ("male", "125.4"),
    ("female", "128.6"))

  val scanLeftExample =
    IterableSource[(String, String)](sampleInput1, ('gender, 'height))
    .groupBy('gender) { group =>
      group.sortBy('height).reverse
      group.scanLeft(('height) -> ('rank))((0L)) {
        (rank: Long, user_id: Double) => { (rank + 1L) }
    }
  }
    // scanLeft generates an extra line per group, thus remove it
    .filter('height) { x: String => x != null }
    .debug
    .write( TextLine( "Output-scanLeft"))



}
