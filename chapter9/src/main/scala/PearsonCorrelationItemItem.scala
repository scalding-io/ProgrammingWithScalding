import com.twitter.algebird.Ring
import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

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
object RingImplicits {
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
}


class PearsonCorrelationItemItem(args: Args) extends Job(args) {
  import RingImplicits._
  import Matrix._

  val matrix = IterableSource(List(
    // User, item, rating
    ("Chris", "Coffee", 2.0),
    ("Jane", "Coffee", 4.0),
    ("Emily", "Coffee", 5.0),
    ("Clint", "Coffee", 4.0),
    ("Jane", "Beer", 0.1),
    ("Emily", "Beer", 4.0),
    ("Clint", "Beer", 5.0)), ('row, 'col, 'val))
    .map('val -> 'val) { v: Double => SingleRating(v) }
    .toMatrix[String, String, MatrixThing]('row, 'col, 'val)

  (matrix.transpose * matrix)
    .mapValues { value: MatrixThing => value.calculateSimilarity }
    .write( Tsv( args("output") ) )
}
