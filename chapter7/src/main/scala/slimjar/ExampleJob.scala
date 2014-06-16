package slimjar

import com.twitter.scalding._

/**
 * Using JobBase we keep a single responsibility in this Job class
 */
 class ExampleJob(args: Args) extends JobBase (args) {

  println("Running a slim jar")

  val simpleExample = Tsv("data/input/", 'num)
      .read
      .write(Tsv("data/output.tsv"))

  }

