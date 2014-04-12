import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix
import com.twitter.algebird.Ring

/**
 * Uses Rings and algebird library
 *
 * Reference code: https://gist.github.com/wibiclint/7711036
 */

/** A vector of ratings for a given item by different users. */
case class ItemRatingVector(ratings: List[Double]) {
  def dotProductWith(that: ItemRatingVector): Double = {
    ratings.zip(that.ratings).map { pair => pair._1 * pair._2 }.sum
  }

  def meanCentered: ItemRatingVector = {
    val avg: Double = ratings.sum / ratings.size
    ItemRatingVector(ratings.map { _ - avg })
  }

  def size: Int = ratings.size
  def magnitude: Double = Math.sqrt(dotProductWith(this))
  def ++(that: ItemRatingVector): ItemRatingVector = {
    ItemRatingVector(ratings ++ that.ratings)
  }
}

/** Superclass use to hold either a single rating or a pair of item-rating vectors. */
abstract class MatrixThing {
  def calculateSimilarity: Double = {
    throw new Exception("This should not happen")
  }
}

/** A single item rating, before any multiplication or addition. */
case class SingleRating(rating: Double) extends MatrixThing

/** A pair of rating vectors (for all common users) for a given pair of items. */
case class RatingVectorPair(vecA: ItemRatingVector, vecB: ItemRatingVector) extends MatrixThing {
  require(vecA.size == vecB.size)

  override def calculateSimilarity: Double = {
    // Center each vector around its mean
    val centeredA = vecA.meanCentered
    val centeredB = vecB.meanCentered

    // Compute the cosine similarity
    centeredA.dotProductWith(centeredB) / (centeredA.magnitude * centeredB.magnitude)
  }
}

object MatrixThingNull extends MatrixThing

class PearsonCorrelationItemSimilarity(args: Args) extends Job(args) {
  import Matrix._
  import com.twitter.scalding.mathematics.MatrixProduct._

  implicit object Ring extends Ring[MatrixThing] {

    /** Multiplication pairs together two ratings with the same user. */
    override def times(l : MatrixThing, r : MatrixThing): MatrixThing = {
      (l, r) match {
        case (SingleRating(a), SingleRating(b)) =>
          RatingVectorPair(ItemRatingVector(List(a)), ItemRatingVector(List(b)))
        case _ => throw new Exception("oopsies!")
      }
    }

    /** Plus should occur only between vectors of rating pairs */
    override def plus(l : MatrixThing, r : MatrixThing): MatrixThing = {
      (l, r) match {
        case (RatingVectorPair(la, lb), RatingVectorPair(ra, rb)) =>
          RatingVectorPair(la ++ ra, lb ++ rb)
        case _ => throw new Exception("oopsies!")
      }
    }

    /** These never get used. */
    override def zero = MatrixThingNull
    override def one = MatrixThingNull
  }

  // User, item, rating
  val input = List(
    // User-1 is a Beer lover
    ("user1", "Beer", 4.0),
    ("user1", "Coffee", 1.0),
    // User-2 and User-3 are Coffee lovers
    ("user2", "Coffee", 4.0),
    ("user3", "Coffee", 5.0),
    ("user2", "Beer", 0.0),
    ("user3", "Beer", 2.2))

  val matrix = IterableSource(input, ('row, 'col, 'val))
    .map('val -> 'val) { v: Double => SingleRating(v) }
    .toMatrix[String, String, MatrixThing]('row, 'col, 'val)

  (matrix * matrix.transpose)
    .mapValues { value: MatrixThing => value.calculateSimilarity }
    .mapValues { value: Double => value }
    .pipe
    // de-duplicate results
    .filter('row,'col) { x:(String,String) => x._1 < x._2 }
    .write( Tsv( "data/output-Pearson-item-similarity.tsv" ) )
  /*
   * We expect negative correlation between Coffee/Beer lovers
   * and positive correlation between Coffee lovers:
   *
   * user1  user2	 -1.0
   * user1  user3	 -1.0
   * user2  user3	  1.0
   */

}
