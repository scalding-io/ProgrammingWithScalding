import com.twitter.scalding.{Job, Args, TextLine, Tsv}
import scala.util.matching.Regex

// readLogFile example
class readLogFile(args: Args) extends Job(args) {

    TextLine("data.log" ).read
      .mapTo('line -> ('productId, 'price, 'quantity)) { x: String => parseLine(x) }
      .write( Tsv( args("output")))

  def parseLine(s : String) = {
    val pattern = new Regex("(?<=\\[)[^]]+(?=\\])")
    (pattern findFirstIn s get ,s.split(" ").toList.get(1) ,s.split(" ").toList.get(2))
  }

}