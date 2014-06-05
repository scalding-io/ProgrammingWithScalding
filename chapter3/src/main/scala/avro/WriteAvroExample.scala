package avro

import com.twitter.scalding._
import com.twitter.scalding.avro._
import org.apache.avro.Schema
import com.gensler.scalavro.types.AvroType
import org.slf4j.LoggerFactory

/**
 * Write some dummy data into Avro UNPACKED format
 *
 * To compress - use Hadoop parameters to specify the codec to be used like:
 *
 * $ hadoop jar chapter3-0-jar-with-dependencies.jar com.twitter.scalding.Tool -Dmapred.output.compress=true -Davro.output.codec=snappy avro.WriteAvroExample --hdfs --output unpacked_data.snappy.avro
 */
class WriteAvroExample(args: Args) extends Job(args) {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  /** Dummy data **/
  val testList = (1 to 100000).toList

  /** Get the Avro schema from the case class **/
  implicit val avroSchema = new Schema.Parser().parse(AvroType[Person].schema.toString)
  log.info("Avro Schema defined in [a Scala case class] is: "+ avroSchema)

  /** Write some data **/
  val avroUnpackedFile = args.getOrElse("output", "unpacked_data.avro")
  log.info(s"Generating [Unpacked] Avro file: $avroUnpackedFile")

  val writeUnpackedAvro = IterableSource[(Int)](testList, ('age))
    .read
    .map('age -> 'name) { x: Int => "Some dummy text"}
    .write(UnpackedAvroSource(avroUnpackedFile, avroSchema))

}