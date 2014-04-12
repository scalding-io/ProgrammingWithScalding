import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

/**
 * In this example, consider the following directed graph:
 *
 *        (3) <- (1) -> (2) - > (4)
 *          \            ^
 *           -----------/
 *
 * There is just one similarity between the edges participating in this directed graph:
 *
 * (1) is similar with (3) with a Jaccard value 0.5 because:
 *
 *  (1) points to (2)
 *  (3) points to (2)
 *
 * We use the Matrix API in this example
 */
class DirectedGraphSimilarity(args: Args) extends Job(args) {

  // The directed graph
  val input = List(
    (1L, 2L, 1D),
    (1L, 3L, 1D),
    (3L, 2L, 1D),
    (2L, 4L, 2D)) // Let's consider that the distance between (2) and (4) is: two

  // Import Matrix._ as matrix calculations will be utilized
  import Matrix._
  val adjacencyMatrix =
    IterableSource[(Long, Long, Double)](input, ('edge1, 'edge2, 'distance))
    .read
    .toMatrix[Long,Long,Double]('edge1, 'edge2, 'distance)
  /*
   * adjacencyMatrix is:
   * 1  2	 1.0
   * 1  3	 1.0
   * 3  2	 1.0
   * 2  4	 2.0
   */

  val binaryMatrix = adjacencyMatrix.binarizeAs[Double]
  /*
   * For sum of columns and sum of rows each element should be counted as one
   * So binarize = transform all distances != 0 to 1
   *
   * binaryMatrix is:
   * 1  2	 1.0
   * 1  3	 1.0
   * 3  2	 1.0
   * 2  4	 1.0    <- This is the only actual change
   */

  val columnSums = binaryMatrix.sumColVectors
  val rowSums = binaryMatrix.sumRowVectors
  /*
   * We use the binaryMatrix to calculate for row and column sums:
   *
   *                1 - 2 - 3 - 4     rowSum
   *             1
   *             2  x       x          2.0
   *             3  x                  1.0
   *             4      x              1.0
   *  columnSum    2.0 1.0 1.0
   */

  val intersectMat = binaryMatrix * binaryMatrix.transpose
  /*
   * intersectMat is A ∩ B
   *
   * in simple words we have flipped the X axis with the Y axis:
   *
   *                1  2	1.0                       2  1  1.0
   * binaryMatrix = 1  3	1.0   ==>  intersectMat = 3  1  1.0
   *                3  2	1.0                       2  3  1.0
   *                2  4  1.0                       4  2  1.0
   */

  val xMat = intersectMat.zip(columnSums)
  /*
   * Used zip, which is an outer join to preserve zeros on either side
   *
   *               2  1  1.0         1  2.0               1  1 (2.0,2.0)
   * intesectMat = 3  1  1.0  .zip ( 2  1.0 ) => xMat =   1  3 (1.0,2.0)
   *               2  3  1.0         3  1.0               2  2 (1.0,1.0)
   *               4  2  1.0                              3  1 (1.0,1.0)
   *
   */
    .mapValues( pair => pair._2 )

  // Same concept for the Y matrix
  val yMat = intersectMat.zip(rowSums)
    .mapValues( pair => pair._2 )

  /**
   * Final step is is to zip (outer-join) the intesection with the union matrix
   * The union matrix can easily be calculated as Doubles are in use
   */
  val unionMat = xMat + yMat - intersectMat
  intersectMat.zip(unionMat)
    // Job done! Let's calculate the Jaccard similarity as as the size of the
    // intersection divided by the size of the union of the sample sets
    .mapValues( pair => pair._1 / pair._2 )
    // Transform Matrix back into a Scalding pipe
    .pipe
    // De-duplicate results and store
    .filter('row, 'col) { x:(String,String) => x._1 < x._2}
    .write(Tsv( "data/output-directed-graph-similarity.tsv" ))

    // Result is:  1  3  0.5
    // well done
}