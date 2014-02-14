package elasticsearch

import com.twitter.scalding._
import org.elasticsearch.hadoop.cascading._

// ElasticSearch example
class ElasticSearchUpdateIndexes(args: Args) extends Job(args) {

  // Some data to push into elastic-search
  val someData = List(
    ("product1", "description1"),
    ("product2", "description2"),
    ("product3", "description3"))

  //val elasticMapping = ( "string", "string/date")
  //val elasticId = Field("product","description")

  // This example is equivalent to using `sum`, but you can also supply other reduce functions.
  val indexNewDataInElasticSearch =
    IterableSource[(String, String)](someData, ('product, 'description))
      .write(elasticsearch.ElasticSearchSource("localhost", 9200,"index_es/type_es"))

//  val readDataFromElasticSearch =
//     ElasticSearchSource("localhost", 9200,"index_es/type_es").read
//     .write(Tsv("dummy"))


}
