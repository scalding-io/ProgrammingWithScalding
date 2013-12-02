import com.twitter.algebird.DenseHLL
import com.twitter.scalding.{Tsv, IterableSource, Job, Args}

class hyperLogLog(args: Args) extends Job(args) {

  // implicit conversion from Object -> Array[Byte] is required
  implicit def utf8ToBytes(s: String) = com.twitter.bijection.Injection.utf8(s)
//  implicit def stringToBytes(text:String) = text.getBytes
  //implicit def intToBytes (s: Int) = com.twitter.bijection.Injection.utf8(s.toString)

  val aList = (0 to 100000).toList

  /** For each key:
    * {{{
    * 10% error ~ 256 bytes
    * 5% error ~ 1kB
    * 2% error ~ 4kB
    * 1% error ~ 16kB
    * 0.5% error ~ 64kB
    * 0.25% error ~ 256kB
    * }}}
    */
  val errPercent = 1D // 1% -> 16kB buffer

  val orderedPipe =
    IterableSource[Int](aList, 'numbers)
    .groupAll { group => group.hyperLogLog[String](('numbers ->'denseHHL),errPercent) }
    .mapTo('denseHHL -> 'approximateSize) { x: DenseHLL => x.approximateSize.estimate }
    .write(Tsv("Output-hyperLogLog"))

  //hyperLogLog[T](f: (Fields, Fields), errPercent: Double = 1.0)(implicit arg0: (T) ⇒ Array[Byte], arg1: TupleConverter[T]): Self


}
