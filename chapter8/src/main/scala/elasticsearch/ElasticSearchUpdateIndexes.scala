package elasticsearch

import com.twitter.scalding._
import cascading.tuple.Fields

// ElasticSearch example
class ElasticSearchUpdateIndexes(args: Args) extends JobBase(args) {

  // Some data to push into elastic-search
  val someData = List(
    ("1","product1", "description11"),
    ("2","product2", "description2"),
    ("3","product3", "description3"))

  val schema = List('number, 'product, 'description)
  val indexNewDataInElasticSearch =
    IterableSource[(String,String, String)](someData, schema)
      .write(ElasticSearchSource("dev.weather.marine.travel", 9200,"index_es/type_es", schema))

//  val readDataFromElasticSearch =
//     ElasticSearchSource("localhost", 9200,"index_es/type_es").read
//     .write(Tsv("dummy"))


}
