import com.twitter.scalding._

// flatMapTo example
class flatMapTo(args: Args) extends Job(args) {

  val kidsList = List(
    ("john", 4, "orange,apple"),
    ("liza", 5, "banana,mango"),
    ("nina", 5 ,"orange"),
    ("nick", 6, ""))

  val pipe =
    IterableSource[(String, Int, String)](kidsList, ('kid, 'age, 'fruits)).read
      .flatMapTo('fruits -> 'fruit) { text : String => text.split(",") }
      .debug
      .write( TextLine( args("output")))


}