import com.twitter.scalding.{Job, Args, TextLine, Tsv}

// flatMap example
class flatMap(args: Args) extends Job(args) {

  val words =
    TextLine( args("input") )
    //.flatMap('line -> 'word) { text : String => text.split("\\s") }
    //.write( Tsv( args("output")))
    .write( TextLine( args("output")))


}
