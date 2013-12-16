import com.twitter.scalding._

// drop
class drop(args: Args) extends Job(args) {

  val aList = List(1,2,3,4,5,6,7,8,9,10)

  val dropExample = IterableSource[(Int)](aList, ('num))
    .read
    .debug
    .groupAll{ group => group.drop(2) }
    .debug
    .write(Tsv("Output-drop"))

}
