import com.twitter.scalding._

// sample example
class sample(args: Args) extends Job(args) {

  val aList = (1 to 100).toList

  // TODO write a Test-Case
  val samplePipe =
    IterableSource[(Int)](aList, ('number))
      .sample(0.1)
      .debug
      .write(Tsv("Output-sample"))

}
