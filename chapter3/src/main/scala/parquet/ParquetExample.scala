package parquet

import _root_.cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.parquet.tuple.ParquetTupleSource

/**
 * A fully working example of reading Parquet files and writing Parquet files
 *
 * Unfortunately vendors are packaging up old versions of parquet libraries that can be found at
 *   /opt/cloudera/parcels/CDH-4.5.0-1.cdh4.5.0.p0.30/lib/parquet
 *   /opt/cloudera/parcels/CDH-4.5.0-1.cdh4.5.0.p0.30/lib/hadoop/
 *
 * And the -Dmapreduce.job.user.classpath.first=true parameter is not respected for the Parquet libs
 * ( issue has been reported with Cloudera )
 *
 * So to make this example run in a Hadoop cluster we actually had to run the script in
 *   src/main/resources/fixParquetLibs.sh
 *
 * Run with parquet.ParquetExample --hdfs --input data/input.parquet --output test
 */
class ParquetExample(args:Args) extends Job(args) {

  val inputSchema = List('id, 'size)
  ParquetTupleSource( inputSchema , args("input") )
    .read
    .groupBy('id) { _.size }
    .write( CustomParquetTupleSource(Fields.ALL, args("output")  ))
    //.write( Tsv( args("output")+".tsv" ) )

}