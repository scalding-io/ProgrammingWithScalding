import com.twitter.scalding._

/**
 * Calculates the Pearson Correlation in the 2D space
 *
 * To understand check: http://neoacademic.com/2009/09/10/a-plain-english-explanation-of-correlation
 *
 * Reference - https://gist.github.com/krishnanraman/7090237
 */
class PearsonCorrelation2D(args:Args) extends Job(args:Args) {
  type X = (Int, Double,Double,Double,Double,Double)
  type T = (Double,Double,Double,Double,Double)

  // Points in X , Y dimension
  val input = List(
    (1D, 2D),
    (1D, 3D),
    (3D, 2D),
    (2D, 4D))

  // Import Matrix._ as matrix calculations will be utilized
  val points =
    IterableSource[(Double, Double)](input, ('x, 'y))
    // Tsv(args("src"), ('x, 'y))

    .map(('x,'y)->('xsq, 'ysq, 'xy)){
      tuple:(Double,Double) =>
        val (x,y) = tuple
        (x*x, y*y, x*y)
    }.groupAll {
      val init:X = (0, 0.0d, 0.0d, 0.0d, 0.0d, 0.0d)
      _.foldLeft(('x,'y,'xsq,'ysq, 'xy) ->('n, 'sigmax, 'sigmay, 'sigmaxsq, 'sigmaysq, 'sigmaxy))(init) {
        (incoming:X, out:T) =>
          val (x, y, xsq, ysq, xy) = out
          val (n, sigmax, sigmay, sigmaxsq, sigmaysq, sigmaxy) = incoming
          (n+1, sigmax + x, sigmay + y, sigmaxsq + xsq, sigmaysq + ysq, sigmaxy + xy)
      }
    }.mapTo(('n, 'sigmax, 'sigmay, 'sigmaxsq, 'sigmaysq, 'sigmaxy)->'rho){
      incoming: X =>
        val (n, sigmax, sigmay, sigmaxsq, sigmaysq, sigmaxy) = incoming
        val num = (n * sigmaxy - sigmax * sigmay)
        val denom = math.sqrt((n * sigmaxsq - sigmax * sigmax) * (n * sigmaysq - sigmay * sigmay))
        num/denom
    }.write(Tsv("data/output-pearson-2D.tsv"))
}

/**
 * Same implementation as above - but in an optimized way
 * Performs better as it generates less map-red tasks
 */
class PearsonCorrelation2DOptimized(args:Args) extends Job(args:Args) {

  // Points in X , Y dimension
  val input = List(
    (1D, 2D),
    (1D, 3D),
    (3D, 2D),
    (2D, 4D))

  // Import Matrix._ as matrix calculations will be utilized
  val points =
    IterableSource[(Double, Double)](input, ('x, 'y)).read
    // Tsv(args("src"), ('x, 'y))

    .flatMapTo(('x,'y)->('key, 'value)){
      tuple:(Double,Double) =>
        val (x,y) = tuple
        Seq(("x", x), ("y", y), ("xsq", x*x), ("ysq", y*y), ("xy", x*y), ("n", 1))
    }.groupBy('key) {
      _.sum[Double]('value)
    }.groupAll {
      _.toList[(String,Double)](('key,'value)->'res)
    }.mapTo('res->'rho){
      x:List[(String,Double)] =>
        println(x)
        val dict = x.toMap
        val (n,sigmax, sigmay, sigmaxsq, sigmaysq, sigmaxy) =
          (dict("n"),dict("x"),dict("y"),dict("xsq"),dict("ysq"), dict("xy"))
        val num = (n * sigmaxy - sigmax * sigmay)
        val denom = math.sqrt((n * sigmaxsq - sigmax * sigmax) * (n * sigmaysq - sigmay * sigmay))
        num/denom
    }.write(Tsv("data/output-pearson-2D-optimized-performance.tsv"))

}
