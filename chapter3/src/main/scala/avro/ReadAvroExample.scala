package avro

import com.twitter.scalding._
import com.twitter.scalding.avro.UnpackedAvroSource

import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema
//import org.slf4j.LoggerFactory

/**
 * We can read both Unpacked Avro files and Snappy compressed Avro files
 * If the input files are compressed, Map-Reduce will automatically decompress
 * them for us
 */
class ReadAvroExample(args: Args) extends Job(args) {

//  val log = LoggerFactory.getLogger(this.getClass.getName)

//  implicit val avroSchema = new Schema.Parser().parse(AvroType[Person].schema.toString)
//  log.info("Avro Schema defined in [a Scala case class] is: "+ avroSchema)

  /**
   * Read data from UnpackedAvro
   */
  val p1 = UnpackedAvroSource("data/avro/part-00000.avro")
    .read
    // We can access anything we need from the Schema automatically i.e.
     .map('name -> 'name2) { x:String => x+ " it works! "}
    // .debug
    .write(Tsv("data/from-avro/"))

  val p2 = UnpackedAvroSource("data/avrosnappy/part-00000.avro")
    .read
    // We can access anything we need from the Schema automatically i.e.
    .map('age -> 'result) { x:String => x+ " it works! "}
    // .debug
    .write(Tsv("data/from-avrosnappy/"))

}