import com.twitter.scalding.{IterableSource, Job, Args, TextLine}

// partition example
class partition(args: Args) extends Job(args) {

  // Imagine a footbal team containing age and weight information
  val footballTeamList = List(
    ("player1", 19, 80),
    ("player2", 17, 74),
    ("player3" ,21, 90))

  val partitionExample =
    IterableSource[(String, Int, Int)](footballTeamList, ('player, 'age, 'weight))
    .partition('age -> 'isAdult) { (_:Int) > 18 } { _.average('weight) }
    .debug
    .write( TextLine( "Output-partition"))

}
