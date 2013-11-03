import com.twitter.scalding.{Job, Args, TextLine, Tsv}
import com.codahale.jerkson.Json._

case class Simple(val productId: String, val price: String, val quantity: String)

// readJSonFile example
class readJSonFile(args: Args) extends Job(args) {

    TextLine("data.json" ).read
      .debug
      .mapTo('line -> ('productId, 'price, 'quantity)) { x: String => parseJSon(x) }
      .write( Tsv( args("output")))

  def parseJSon (s : String) = {
    val simple = parse[Simple](s)
    (simple.productId , simple.price, simple.quantity)
  }

}