import com.twitter.scalding._

// addTrap product example
class addTrap(args: Args) extends Job(args) {

  val numbersList = List(
    (10, 2),
    (10, 1),
    (10, 0)) // division by zero exception

  val pipe =
    IterableSource[(Int, Int)](numbersList, ('num1, 'num2)).read
     .map(('num1,'num2)-> 'div) {
       x:(Int,Int) => x._1 / x._2
     }
     .addTrap(Tsv("output-trap"))
     .write(Tsv("output"))

      /** output-trap will contain the tuples that resulted in an exception i.e.
      10 0
      */
}
