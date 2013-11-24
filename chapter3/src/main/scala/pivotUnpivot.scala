import com.twitter.scalding._

class pivotUnpivot(args: Args) extends Job(args) {

  val salesList = List(
    ("quarter1",  "wine",   100),
    ("quarter1",  "beer",   220),
    ("quarter1",  "coffee", 550),
    ("quarter2",  "coffee", 5))

  val defaultValue = 0

  val pivotPipe =
    IterableSource[(String, String, Int)](salesList, ('quarter, 'product, 'sales))
        .groupBy('quarter) { group => group.pivot(('product,'sales) -> ('wine, 'beer,'coffee), defaultValue) }
      .debug
      .write(Tsv("pivotOutput"))

  val unpivotPipe =
    pivotPipe.unpivot(('wine, 'beer,'coffee) -> ('product, 'sales))
      .write(Tsv("unpivotOutput"))
}
