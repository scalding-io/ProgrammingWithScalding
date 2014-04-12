import com.twitter.scalding._
import com.twitter.algebird.{ MinHasher, MinHasher32, MinHashSignature }

/**
 *
 * THEORY:
 * ------
 * Locality Sensitive Hashing is a good way to find similar items with high-dimension properties.
 * An example of high-dimension is a forensic team finding a finger-tip in a crime scene, and collecting
 * a few thousand data-points of that specific finger-tip. LSH is an excellent approach, as we can
 * examine 1/250th of the database and still get ~ 97% accurate result. So similarly to HyperLogLog
 * there is approximation in the calculations.
 *
 * This implementation of MinHasher in algebird is modeled after Chapter 3 of Ullman and Rajaraman's
 * Mining of Massive Datasets:
 *
 * http://infolab.stanford.edu/~ullman/mmds/ch3a.pdf
 *
 * To understand LSH see:
 *
 * http://www.cs.jhu.edu/~vandurme/papers/VanDurmeLallACL10-slides.pdf
 * http://stackoverflow.com/questions/12952729/how-to-understand-locality-sensitive-hashing
 *
 * Computes similar items (with a string itemId), based on approximate
 * Jaccard similarity, using LSH.
 *
 * Generates an output file of the following format:
 *
 *    itemId   similarItemId   similarity
 *
 */
class LocalitySensitiveHashingSimilarity(args: Args) extends Job(args) {

  /*
   * Approximate estimations - More means higher complexity, but higher accuracy
   */
  val numHashes = args.optional("num_hashes").map { _.toInt }.getOrElse(1000)

  /*
   * Suppose we have a million of documents to compare. If we hash each document to 1 KByte
   * the entrire data-set becomes just 1 GByte. However we still have half a trillion pairs of
   * documents to compare. As most often we are interested in the most similar pairs of all pairs, we
   * can set a threshold and focus only on pairs that are likely to be similar, without investigating
   * every pair.
   *
   * Minimum Jaccard similarity above which two items are considered similar
   */
  val targetThreshold = args.optional("target_threshold").map {_.toDouble }.getOrElse(0.2)

  // MinHash functions are efficient, since we can hash sets in time,
  // proportional to the size of the data
  val numBands = MinHasher.pickBands(targetThreshold, numHashes)
  implicit lazy val minHasher = new MinHasher32(numHashes, numBands)

  val input = List(
    ("item1", 1L),
    ("item2", 1L),
    ("item3", 1L),
    ("item1", 2L),
    ("item2", 2L),
    ("item10", 2L))

  val users_items =
    IterableSource[(String, Long)](input, ('item, 'user))
    .read
    .map('user -> 'hash) { x:Long => minHasher.init(x) }
    .groupBy('item) { group => group.sum('hash) }
    .flatMapTo(('item, 'hash)-> ('bucket, 'index, 'set)) {
       x:(String,MinHashSignature) => {
         val itemId = x._1
         val hash = x._2
         minHasher.buckets(hash).zipWithIndex.map {
           case (bucket, ind) =>  (bucket, ind, Set((itemId, hash)))
         }
       }
    }
    .groupBy('bucket) { _.sum[(Set[(String,MinHashSignature)])]('set) }
    .flatMapTo[Set[(String,MinHashSignature)],(String,String, Double)]('set -> ('item1,'item2,'similarity)) {
      case (itemIdSet) =>
        for {
          (itemId1, sig1) <- itemIdSet
          (itemId2, sig2) <- itemIdSet
          sim = minHasher.similarity(sig1, sig2)
          if (itemId1 < itemId2 && sim >= targetThreshold)
        } yield (itemId1, itemId2,sim)
    }
    .unique('item1,'item2,'similarity)
    .write(Tsv("data/output-LSH-Jaccard.tsv"))

}
