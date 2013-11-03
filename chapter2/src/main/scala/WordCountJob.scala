import com.twitter.scalding.{Job, Args, TextLine, Tsv}

class WordCountJob(args: Args) extends Job(args) {

  TextLine( args("input") )
      .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )

  def tokenize(text : String) : Array[String] = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
