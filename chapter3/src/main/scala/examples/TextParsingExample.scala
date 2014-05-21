package examples

import com.twitter.scalding._
import scala.util.matching.Regex

class TextParsingExample (args: Args) extends Job(args) {

  /** Input data:
   [product15] 10.0 11
   [product16] 2.5 29
    */
  val pipe = TextLine ( args("input") ).read
    .name("Text Parsing Pipe")
    .mapTo ('line -> ('productID, 'price, 'quantity))
  {  x: String => parseLine(x)  }
    .debug
    .write ( Tsv ( args("output") ) )

  val pattern = new Regex("(?<=\\[)[^]]+(?=\\])")
  def parseLine(s : String) = {
    ( pattern findFirstIn s get ,   // 1st tuple element
      s.split(" ").toList.get(1),   // 2nd tuple element
      s.split(" ").toList.get(2)  ) // 3rd tuple element
  }
}

