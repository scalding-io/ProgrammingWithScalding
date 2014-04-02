import com.twitter.scalding._
//import com.twitter.scalding.avro.{PackedAvroSource, UnpackedAvroSource}

//import com.gensler.scalavro.types._
//import org.apache.avro.Schema
//import org.apache.avro.specific.{SpecificRecord, SpecificRecordBase}
//import test.avro.oogway_session_fact

case class Person(name: String, age: Int)
//  extends SpecificRecord {
//  def put(field: Int, value: scala.Any) = {
//    val fields = (this.getClass.getDeclaredFields())
//    fields(field).set(this,value)
//  }
//  def getSchema: Schema = new Schema.Parser().parse(AvroType[Person].schema.toString)
//  def get(field: Int): AnyRef = {
//    if (field == 1)
//      name
//    else
//      age
////    val fields = this.ggetDeclaredFields()
////    fields(field).get(this)
//  }
//}


/**
 * hadoop jar chapter3-0-jar-with-dependencies.jar com.twitter.scalding.Tool -Dmapred.output.compress=true AvroExample --hdfs
 *
 */
class AvroExample(args: Args) extends Job(args) {

//  // First write the 2 avro files , then change this to false...
//  var write = true
//
//  /**
//   * Dummy data
//   */
//  val testList = (1 to 100000).toList
////  val testList = List(
////    ("name1", 10),
////    ("name2", 20),
////    ("name3" ,30))
//
//
//  implicit val avroSchema = new Schema.Parser().parse(AvroType[Person].schema.toString)
////  println("--------->"+ conf.get(AvroJob.OUTPUT_CODEC));
//
//  if (write) {
//  // --- WRITING AVROs
//  // -------------------------------- UNPACKED AVRO ---------------------------------
//
//  /**
//    * Write dummy data to UnpackedAvro
//    */
//  println("-->"+avroSchema)
//
//  val writeToUnpackedAvro =
//    IterableSource[(Int)](testList, ('age))
//      .read
//      .map('age -> 'name) { x: Int => "My name is lalallalalallalalalallalalallalalalalallalalalalal" + x}
//      //.debug
//      .write(UnpackedAvroSource("write-unpacked-avro", avroSchema))
//
//  // -------------------------------- PACKED AVRO ---------------------------------
//
//  /**
//    * Write dummy data to PackedAvro
//    */
//  val writeToPackedAvro =
//    IterableSource[Int](testList, ('age)).read
//      .map('age -> 'name) { x: Int => "My name is lalallalalallalalalallalalallalalalalallalalalalal" + x}
//      .mapTo(('name, 'age) -> 'p) {
//        t:(String, Int) => {
//          Person(t._1, t._2)
//         val oog = new oogway_session_fact()
//          oog.setCHARKYFLDUSERDIM(t._1)
//          oog.setBATCHNUMBER(t._2)
//          oog
//        }
//      }
//      .project('p)
//      .debug
//      .write(PackedAvroSource[oogway_session_fact]("write-packed-avro"))
//
//  } else {
//
//  /**
//   * Read data from PackedAvro
//   */
//  PackedAvroSource[oogway_session_fact]("write-packed-avro")
//    .read
//    .debug
//    .write(Tsv("tsv-packed"))
//
//  /**
//   * Read data from UnpackedAvro
//   */
//  UnpackedAvroSource("write-unpacked-avro")
//    .read
//    .debug
//    .write(Tsv("tsv-unpacked"))
//
//  }
//
//  // -------------------------- To compress ---------------------------------------
//  // Use : hadoop jar chapter3-0-jar-with-dependencies.jar com.twitter.scalding.Tool -Dmapred.output.compress=true -Davro.output.codec=snappy AvroExample --hdfs

}