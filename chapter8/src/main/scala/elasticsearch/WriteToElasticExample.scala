package elasticsearch

import com.twitter.scalding._

// ElasticSearch example
class WriteToElasticExample(args: Args) extends Job(args) {

  // Some data to push into elastic-search
  val someData = List(
    ("1","product1", "description11"),
    ("2","product2", "description2"),
    ("3","product3", "description3"))

  val schema = List('number, 'product, 'description)
  val indexNewDataInElasticSearch =
    IterableSource[(String,String, String)](someData, schema)
      .write(ElasticSearchTap("localhost", 9200,"index_es/type_es", schema))

}
