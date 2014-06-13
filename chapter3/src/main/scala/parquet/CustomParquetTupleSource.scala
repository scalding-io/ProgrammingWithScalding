package parquet

import _root_.cascading.scheme.Scheme
import _root_.cascading.tuple.Fields
import com.twitter.scalding.{FixedPathSource, HadoopSchemeInstance, FileSource}
import parquet.cascading.ParquetTupleScheme
import parquet.schema.MessageTypeParser

/**
 *
 * TODO : extends https://github.com/Parquet/parquet-mr/blob/master/parquet-cascading/src/main/java/parquet/cascading/ParquetTupleScheme.java
 * and add the new Constructor (!)
 *
 *
 * Here we define
 *  i) The Parquet schema to be used in this job
 *  ii) User should define their own source like:
 * class MySource(path: String, dateRange: DateRange, requestedFields: Fields) extends DailySuffixParquetTuple(path, dateRange, requestedFields) with Mappable2[Int, Int] with TypedSink2[Int,Int]
 */
trait CustomParquetTupleSource extends FileSource {
  // Define the parquet schema
  val parquet_schema = MessageTypeParser.parseMessageType(
    "message parquet_test { required int32 id;required int32 size;}"
  )
  print(s"Parquet Schema ${parquet_schema.toString}")
  // Override fields
  def fields: Fields
  // Define Parquet INPUT and Parquet OUTPUT scheme
  override def hdfsScheme = HadoopSchemeInstance(new ParquetTupleScheme(new Fields("id", "size"),new Fields("id", "size"), parquet_schema.toString()).asInstanceOf[Scheme[_, _, _, _, _]])
}
object CustomParquetTupleSource {
  def apply(fields: Fields, paths: String*) = new FixedPathParquetTap(fields, paths: _*)
}
class FixedPathParquetTap(override val fields: Fields, paths: String*)
  extends FixedPathSource(paths: _*) with CustomParquetTupleSource