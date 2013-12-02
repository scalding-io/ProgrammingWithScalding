import com.twitter.scalding.{IterableSource, Job, Args, TextLine}

// Test heap space
class testHeapSpace(args: Args) extends Job(args) {

  val key = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val aList = (0 to 100000).toList

  val partitionExample =
    IterableSource[(Int)](aList, ('number))
    //.partition('age -> 'isAdult) { (_:Int) > 18 } { _.average('weight) }
    .debug
    .write( TextLine( "Output-partition"))

}
