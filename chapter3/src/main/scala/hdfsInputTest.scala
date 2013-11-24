import com.twitter.scalding._

class hdfsInputTest(args: Args) extends Job(args) {

  val pipe =
    Tsv(args("input") , 'text)
      .write(Tsv(args("output")))
}

