import com.twitter.scalding._

// This is equivalent to a Java class with three attributes and the relevan setters
case class Person(productID : Long, price : Float, quantity : Int)

// pack example
class pack(args: Args) extends Job(args) {

  val aList = (1 to 100).toList

  // TODO write a Test-Case
  val samplePipe =
    IterableSource[(Int)](aList, ('number))
      .sample(0.1)
      .debug
      .write(Tsv("Output-sample"))

}
