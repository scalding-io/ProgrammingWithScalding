package elasticsearch

import com.twitter.scalding._
import cascading.tuple.Fields
import java.util.Properties
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

// ElasticSearch example
class ElasticSearchUpdateIndexes(args: Args) extends Job(args) {

  // Some data to push into elastic-search
  val someData = List(
    ("1","product1", "description11"),
    ("2","product2", "description2"),
    ("3","product3", "description3"))

  val elasticSearchProperties = new Properties()
  elasticSearchProperties.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, "index")
  elasticSearchProperties.setProperty(ConfigurationOptions.ES_MAPPING_ID     , "number")

//  elasticSearch

  val schema = List('number, 'product, 'description)
  val indexNewDataInElasticSearch =
    IterableSource[(String,String, String)](someData, schema)
      .write(ElasticSearchSource("dev.weather.marine.travel", 9200,"index_es/type_es", schema, elasticSearchProperties))

//  val readDataFromElasticSearch =
//     ElasticSearchSource("localhost", 9200,"index_es/type_es").read
//     .write(Tsv("dummy"))


}
