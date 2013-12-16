import com.twitter.scalding._

// crossWithTiny example
class crossWithTiny(args: Args) extends Job(args) {

  val movieMetadataList = List(
    (1, "title1"),
    (2, "title2"),
    (3, "title3"))

  val moviesWatchedPipe = List(
    ("user1", 1),
    ("user2", 1),
    ("user2", 2),
    ("user3", 4))

  val movieMetadataPipe =
    IterableSource[(Int,String)](movieMetadataList, ('movieId,'title))

  val movieWatchedPipe =
    IterableSource[(String, Int)](moviesWatchedPipe, ('userId,'movieId_))

  val result =  movieMetadataPipe.crossWithTiny(movieWatchedPipe)
    .debug
    .write(Tsv("Output-crossWithTiny"))
}
