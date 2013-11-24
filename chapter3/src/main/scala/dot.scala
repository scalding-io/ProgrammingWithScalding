import com.twitter.scalding._

// dot product example
class dot(args: Args) extends Job(args) {

  // A set of projects. Each project requires a number of steps
  // Each step has a specific cost associated to it
  val squaresList = List(
    ("blueSquare", 2, 10),
    ("blueSquare", 1,  5),
    ("redSquare" , 2 , 3))

  // This example is equivalent to using `sum`, but you can also supply other reduce functions.
  val costPerProjectPhase =
    IterableSource[(String, Int, Int)](squaresList, ('square, 'x, 'y))
      .groupBy('square) { group => group.dot[Int]('x, 'y, 'x_dot_y) }
      .debug
      .write(Tsv("dot"))

}

