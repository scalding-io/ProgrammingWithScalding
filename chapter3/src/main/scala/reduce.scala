import com.twitter.scalding._

// reduce example
class reduce(args: Args) extends Job(args) {

  // A set of projects. Each project requires a number of steps
  // Each step has a specific cost associated to it
  val boxesList = List(
    ("box1", "width", 10),
    ("box1", "height", 5),
    ("box1", "depth" , 2),
    ("box2", "width",  1),
    ("box2", "height", 2),
    ("box2", "depth" , 3))

  // This example is equivalent to using `sum`, but you can also supply other reduce functions.
  val costPerProjectPhase =
    IterableSource[(String, String, Int)](boxesList, ('project, 'dimension, 'length))
      .groupBy('project) { group =>
         group.reduce('length -> 'volume) {
             (volumeSoFar : Int, length : Int) => volumeSoFar * length
         }
      }
      .debug
      .write(Tsv("reduce"))

}
