import com.twitter.scalding._

// sortedReverseTake example
class sortedReverseTake(args: Args) extends Job(args) {

  val kidsList = List(
    ("john", 4),
    ("liza", 5),
    ("nina", 5),
    ("nick", 6))

  val pipe =
    IterableSource[(String, Int)](kidsList, ('kid, 'age)).read
      .groupAll { group => group.sortedReverseTake [(Int,String)] (('age, 'kid)->'newList, 2) }
      .debug
      .write( TextLine(args("output")))

}