import com.twitter.scalding.{Job, Args, TextLine, Tsv}

class WordCountJob(args : Args) extends Job(args) {
  TextLine( args("input") )
    .flatMap('line -> 'word) { line : String =>
      line.toLowerCase.split("\\s+")
    }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )
}
