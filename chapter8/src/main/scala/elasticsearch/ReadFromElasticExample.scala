package elasticsearch

import com.twitter.scalding._

// ElasticSearch example
class ReadFromElasticExample(args: Args) extends Job(args) {

  val schema = List('number, 'product, 'description)

  val fromESpipe =
     ElasticSearchTap("localhost", 9200,"index_es/type_es","", schema).read
     .write(Tsv("data/es-out.tsv"))

}
