import com.twitter.scalding._
import com.twitter.algebird.{ MinHasher, MinHasher32, MinHashSignature }

/**
 * Implementation of LSH & Jaccard, using the Typed API
 * kudos @argyris - reference https://gist.github.com/azymnis/7940080
 *
 * Generates an output file of the following format:
 *
 *    itemId   similarItemId   similarity
 */
class LocalitySensitiveHashingSimilarityTypedAPI(args: Args) extends Job(args) {

  val numHashes = args.optional("num_hashes").map { _.toInt }.getOrElse(100)

  val targetThreshold = args.optional("target_threshold").map {_.toDouble }.getOrElse(0.2)

  val numBands = MinHasher.pickBands(targetThreshold, numHashes)
  implicit lazy val minHasher = new MinHasher32(numHashes, numBands)

  // Input is 'itemId, 'userId
  val input = List(
    ("item1", 1L),
    ("item2", 1L),
    ("item3", 1L),
    ("item1", 2L),
    ("item2", 2L),
    ("item10", 2L))

  val users_items =
    TypedPipe.from(IterableSource[(String, Long)](input, ('item, 'user)))
    // To read from file use:
    // TypedTsv[(String, Long)]("data/item-user.tsv").read

    // First generate minhash signatures
    .map { case (itemId, userId) => (itemId, minHasher.init(userId)) }
    .group[String, MinHashSignature]
    .sum
    // Generate bands of similar items and aggregate
    .flatMap { case (itemId, sig) =>
      minHasher.buckets(sig).zipWithIndex.map {
        case (bucket, ind) => ((bucket, ind), Set((itemId, sig)))
      }
    }
    .group[(Long, Int), Set[(String, MinHashSignature)]]
    .sum
    // Expand all pairs of similar items
    .flatMap { case (_, itemIdSet) =>
      for {
        (itemId1, sig1) <- itemIdSet
        (itemId2, sig2) <- itemIdSet
        sim = minHasher.similarity(sig1, sig2)
        if (itemId1 < itemId2 && sim >= targetThreshold)
      } yield (itemId1, itemId2,sim)
    }
    .distinct
    .write(TypedTsv[(String, String,Double)]("data/output-LSH-Jaccard-TypedAPI.tsv"))

}