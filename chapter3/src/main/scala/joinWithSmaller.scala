import cascading.pipe.joiner.{OuterJoin, RightJoin, LeftJoin}
import com.twitter.scalding.{Tsv, IterableSource, Job, Args}

class joinWithSmaller(args: Args) extends Job(args) {

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
    IterableSource[(String, Int)](moviesWatchedPipe, ('userId,'movieId))

  // By default all joins are inner-joins
  val joinedPipe = movieMetadataPipe.joinWithSmaller('movieId -> 'movieId, movieWatchedPipe)
    .write(Tsv("Output-joinWithSmaller"))


  // By specifying the joiner , left/right/outer joins can be achieved
  // In that case join keys must be disjoint
  // To overcome, rename the key in one pipe
  val movieWatchedPipe_ = movieWatchedPipe.rename('movieId -> 'movieId_)

  val leftJoinedPipe = movieMetadataPipe.joinWithSmaller('movieId -> 'movieId_, movieWatchedPipe_, joiner= new LeftJoin)
    .write(Tsv("Output-leftJoinWithSmaller"))

  val rightJoinedPipe = movieMetadataPipe.joinWithSmaller('movieId -> 'movieId_, movieWatchedPipe_, joiner= new RightJoin)
    .write(Tsv("Output-rightJoinWithSmaller"))

  val outerJoinedPipe = movieMetadataPipe.joinWithSmaller('movieId -> 'movieId_, movieWatchedPipe_, joiner= new OuterJoin)
    .map('title -> 'title2) { x:String => x+"a" }
    .write(Tsv("Output-outerJoinWithSmaller"))

}
