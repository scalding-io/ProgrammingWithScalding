package coordinating

import com.twitter.scalding._

/**
 * A dummy job
 */
class JobA(args: Args) extends Job(args) {

    IterableSource[(String)](List(("a")), ('name)).read
      .write(Tsv("data/output2.tsv"))
}