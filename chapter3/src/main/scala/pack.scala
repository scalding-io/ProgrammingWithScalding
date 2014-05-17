import com.twitter.scalding._

// This is equivalent to a Java class with three attributes and the relevant setters
case class Product(productID : Long =0L, price : Double =0, quantity : Int = 0)

// pack and unpack example
class pack(args: Args) extends Job(args) {

  // --- A simple ranking job
  val sampleInput1 = List(
    (1001L, 10.0 , 30),
    (1002L,  5.0 , 40),
    (1003L,  3.0 , 50))

  val pipe =
    IterableSource[(Long, Double, Int)](sampleInput1, ('productID, 'price, 'quantity))
      .pack [ Product ] ( ('productID,'price,'quantity)->'product )
      .debug
      .write(Tsv("Output-pack"))
    /**
     Result:
  1001	10.0	30	Product(1001,10.0,30)
  1002	5.0	  40	Product(1002,5.0,40)
  1003	3.0	  50	Product(1003,3.0,50)
      */

  // now un-pack
  val unpacked_pipe = pipe
      .project('product)
      .unpack [Product] ('product -> ('productID,'price,'quantity) )
      .debug
      .write(Tsv("Output-unpack"))

   /**
    Result:

   	Product(1001,10.0,30)  1001	10.0	30
    Product(1002,5.0,40)   1002	5.0	  40
    Product(1003,3.0,50)   1003	3.0	  50
    */

}
