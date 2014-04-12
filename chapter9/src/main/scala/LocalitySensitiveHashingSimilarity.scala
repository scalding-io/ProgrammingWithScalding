/**
 * Comes from - https://gist.github.com/azymnis/7940080
 */
import com.twitter.scalding._
import com.twitter.algebird.{ MinHasher, MinHasher32, MinHashSignature }

/**
 * Computes similar items (with a string itemId), based on approximate
 * Jaccard similarity, using LSH.
 *
 * Assumes an input data TSV file of the following format:
 *
 *    itemId   userId
 *
 * Generates an output file of the following format:
 *
 *    itemId   similarItemId
 *
 * Input arguments:
 *    --input              location of input file
 *    --output             location of output file
 *    --num_hashes         number of hash functions to use, more means
 *                         higher complexity, but higher accuracy
 *    --target_threshold   minimum Jaccard similarity above which two
 *                         items are considered similar
 *
 */
class LocalitySensitiveHashingSimilarity(args: Args) extends Job(args) {
  import TDsl._

  val targetThreshold = args.optional("target_threshold")
    .map { _.toDouble }.getOrElse(0.8)

  val numHashes = args.optional("num_hashes")
    .map { _.toInt }.getOrElse(50)

  val numBands = MinHasher.pickBands(targetThreshold, numHashes)
  implicit lazy val minHasher = new MinHasher32(numHashes, numBands)

  TypedTsv[(String, Long)](args("input"))

    // First generate minhash signatures
    .map { case (itemId, userId) => (itemId, minHasher.init(userId)) }
    .group[String, MinHashSignature]
    .sum

    // Now generate bands of similar books and aggregate
    .flatMap { case (itemId, sig) =>
    minHasher.buckets(sig).zipWithIndex.map { case (bucket, ind) =>
      ((bucket, ind), Set((itemId, sig)))
    }
  }
    .group[(Long, Int), Set[(String, MinHashSignature)]]
    .sum

    // Now expand all pairs of similar books
    .flatMap { case (_, itemIdSet) =>
    for {
      (itemId1, sig1) <- itemIdSet
      (itemId2, sig2) <- itemIdSet
      sim = minHasher.similarity(sig1, sig2)
      if (itemId1 != itemId2 && sim >= targetThreshold)
    } yield (itemId1, itemId2)
  }
    .distinct
    .write(TypedTsv[(String, String)](args("output")))
}
