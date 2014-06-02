package kmeans

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.mahout.clustering.kmeans.{RandomSeedGenerator, KMeansDriver}
import org.apache.mahout.common.distance.EuclideanDistanceMeasure

import com.twitter.scalding._
import org.apache.mahout.math.{NamedVector, DenseVector, VectorWritable}
import com.twitter.scalding.IterableSource
import org.apache.mahout.common.HadoopUtil
import org.apache.mahout.clustering.iterator.ClusterWritable

/**
 * Purpose of this Machine-Learning job is to identify a cluster of users with legitimate behavior,
 * and highlight all the out-liers
 */

// Simple package object to hold configuration
package object KMConfig {
  val MAHOUT_VECTORS   = "data/kmeans/mahout_vectors"
  val RANDOM_CENTROIDS = "data/kmeans/random_centroids"
  val RESULT_CLUSTER   = "data/kmeans/result_cluster"
  val RESULT_DISTANCES = "data/kmeans/output-distances.tsv"
}

/**
 * Simple Scalding job to ETL data - into Mahout compatible format
 *
 * K-means algorithm requires the input to be represented as vectors [Text, VectorWritable]
 *    [Text] is the user
 *    [VectorWritable] contains 'features' for that user
 */
class ETLJob(args: Args) extends Job(args) {

  val input = List(
    // User - Features ( login-events, wrong-password, reset-password, post-article )
    ("user1", "1,0,0,15"),
    ("user2", "1,0,1,0"),
    ("user3", "1,0,0,0"),
    ("user4", "1,0,0,2"),
    ("user5", "1,3,0,0"),
    ("user6", "1,0,0,2"),
    ("user7", "4,0,0,0"))

  // Pipe to transform comma-separated features into a dense vector
  val userFeatures =
    IterableSource[(String, String)](input, ('user -> 'features))
    .read
    .mapTo(('user, 'features) ->('user, 'vector)) {
      x: (String, String) =>
        val user = x._1
        val all_features = x._2.split(",").map(_.toDouble)

        val namedVector = new NamedVector(new DenseVector(all_features), user)
        val vectorWritable = new VectorWritable(namedVector)
        println(user + " named-vector-> " + namedVector.toString)
        // Emit [Text, VectorWritable]
        (new Text(user), vectorWritable)
    }
    .project('user -> 'vector)

  val out = WritableSequenceFile[Text, VectorWritable](KMConfig.MAHOUT_VECTORS, 'user -> 'vector)
  userFeatures.write(out)

}

/**
 * Randomly selected cluster the will be passed as an input to K-means
 */
object RandomCentroid  extends App {
    println("Generating random centroid to use as seed in K-Means")

    val conf = new Configuration
    conf.set("io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization," +
      "org.apache.hadoop.io.serializer.WritableSerialization")
    val inputClustersPath = new Path(KMConfig.RANDOM_CENTROIDS)
    val distanceMeasure = new EuclideanDistanceMeasure
    val vectorsPath = new Path(KMConfig.MAHOUT_VECTORS)
    RandomSeedGenerator.buildRandom(conf, vectorsPath, inputClustersPath, 1, distanceMeasure)

 }

object KMeans extends App {
    println("Running K-Means !")
    val conf = new Configuration

    // Delete any existing results (from previous execution)
    HadoopUtil.delete(conf, new Path(KMConfig.RESULT_CLUSTER));

    // Runs K-means algorithm with up to 20 iterations
    KMeansDriver.run(
      conf,
      new Path(KMConfig.MAHOUT_VECTORS),   // INPUT
      new Path(KMConfig.RANDOM_CENTROIDS),
      new Path(KMConfig.RESULT_CLUSTER),   // OUTPUT_PATH
      0.01,                       //convergence delta
      20,                         // MAX_ITERATIONS
      true,                       // run clustering
      0,                          // cluster classification threshold
      false)                      // runSequential
    
}

class FinalJob(args: Args) extends Job(args) {

  // Mahout adds a -final in the final cluster
  val finalClusterPath = KMConfig.RESULT_CLUSTER + "/*-final"

  val finalCluster = WritableSequenceFile[IntWritable, ClusterWritable](finalClusterPath, 'clusterId -> 'cluster)

  val clusterCenter = finalCluster
    .read
    .map('cluster -> 'center) {
      x: ClusterWritable =>
        val center = x.getValue.getCenter
        println("cluster center -> " + center + " size -> " + center.size)
        center
      }

  val userVectors  = WritableSequenceFile[Text, VectorWritable](KMConfig.MAHOUT_VECTORS, 'user -> 'vector)
    .crossWithTiny(clusterCenter)
    .map(('center, 'vector) -> 'distance) {
      x:(DenseVector, VectorWritable) =>
        (new EuclideanDistanceMeasure()).distance(x._1, x._2.get)
    }
    .project('user, 'distance)
    //.debug
    // TODO - Report bug in Scalding 0.9.1 on --local
    // the following line messes up the 'user
    .groupAll { group => group.sortBy('distance).reverse }
    .debug
    .write(Tsv(KMConfig.RESULT_DISTANCES))
  /*
   * user1, 12.301484894607029
   * user5, 3.7661218075611624
   * user7, 3.7661218075611624
   * user2, 2.9102212553519093
   * user3, 2.7847983842311326
   * user4, 0.9476070829586857
   * user6, 0.9476070829586857
   */

}

/**
 * Remember - use Mahout in local mode if crunching less than ~ 250 MBytes of data
 * Map-Reduce gives benefits on 'big-data' when you are facing clustering features of > 1 GB data
 */