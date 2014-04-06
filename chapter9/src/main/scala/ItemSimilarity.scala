import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

/**
 */
class ItemSimilarity(args: Args) extends Job(args) {

  import Matrix._

  // Step 1 - Read input and transform into lines : (book - word)
  val inputSchema = List('user, 'item)
  val adjacencyMatrix = Tsv("data/users-items.tsv", inputSchema)
    .read
    .insert('rel, 1)
    .toMatrix[String,String,Double]('user, 'item, 'rel)

  val aBinary = adjacencyMatrix.binarizeAs[Double]

  // intersectMat holds the size of the intersection of row(a)_i n row (b)_j
  val intersectMat = aBinary * aBinary.transpose
    .write(Tsv("data/output-intersectMat"))
  val aSumVct = aBinary.sumColVectors
    .write(Tsv("data/output-sumCol"))
  val bSumVct = aBinary.sumRowVectors
    .write(Tsv("data/output-sumRow"))


  //Using zip to repeat the row and column vectors values on the right hand
  //for all non-zeroes on the left hand matrix
  val xMat = intersectMat.zip(aSumVct).mapValues( pair => pair._2 )

  val yMat = intersectMat.zip(bSumVct).mapValues( pair => pair._2 )

  val unionMat = xMat + yMat - intersectMat
  //We are guaranteed to have Double both in the intersection and in the union matrix
  intersectMat.zip(unionMat)
    .mapValues( pair => pair._1 / pair._2 )
    .pipe
    .filter('row, 'col) { x:(String,String) => x._1 < x._2}
    .write(Tsv( "data/output-is-result.tsv" ))

}