package elasticsearch

import com.twitter.scalding._
import org.elasticsearch.hadoop.cascading._
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.mapred.JobConf
import cascading.tuple.Fields

// ElasticSearch example
class ElasticSearchUpdateIndexes(args: Args) extends Job(args) {

  // Prior to 0.9.0 we need the mode, after 0.9.0 mode is a def on Job.
//  override def config(implicit m: Mode): Map[AnyRef,AnyRef] = {
//    super.config     ++ Map ("es.mapping.id" -> "uuid")
//    //++ Map("es.mapping.id.extractor.class" -> "String")
//    //++ Map ("es.write.operation" -> "create")
//  }

  // Some data to push into elastic-search
  val someData = List(
    (1,"product1", "description11"),
    (2,"product2", "description2"),
    (3,"product3", "description3"))

  val indexNewDataInElasticSearch =
    IterableSource[(Int,String, String)](someData, ('uuid, 'product, 'description))
      .write(ElasticSearchSource("localhost", 9200,"index_es/type_es"))

//  val readDataFromElasticSearch =
//     ElasticSearchSource("localhost", 9200,"index_es/type_es").read
//     .write(Tsv("dummy"))


}
