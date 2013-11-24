import com.twitter.scalding._

// foldLeft example
class foldLeft(args: Args) extends Job(args) {

  // A set of shopping carts.
  // Each product has an original price and a discount
  val cartList = List(
    ("cart1", 100.0, 10),   // 90
    ("cart1", 10.0,  10),   // 9
    ("cart1", 10.0 , 90),   // 1
    ("cart2", 50.0,  20),   // 40
    ("cart2", 50.0,  10),   // 45
    ("cart2", 25.0,  20))   // 20

  // This example is equivalent to using `sum`, but you can also supply other reduce functions.
  val costPerProjectPhase =
    IterableSource[(String, Double, Int)](cartList, ('cart, 'price, 'discount))
    .groupBy('cart) {
      val initial_cost = 5.0
      group => group.foldLeft(('price, 'discount)-> 'total_cost)(initial_cost) {
        (initial_cost: Double, input: (Double, Double)) =>
          val (price, discount) = input
          initial_cost + price * ( 100 - discount )/100
      }
    }
    .debug
    .write(Tsv("foldLeft"))


}

