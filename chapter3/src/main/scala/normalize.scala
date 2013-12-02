import com.twitter.scalding.{IterableSource, Job, Args, TextLine}

// normalize example
class normalize(args: Args) extends Job(args) {

  // Each step has a specific cost associated to it
  val pokerList = List(
    ("player1", 90),
    ("player2", 2),
    ("player3" ,8))

  // Player
  val costPerProjectPhase =
    IterableSource[(String, Int)](pokerList, ('player, 'money))
    .normalize('money)
    .debug
    .write( TextLine( "Output-normalize"))

}
