package hbasespyglass

import com.twitter.scalding.{TextLine, Job, Args}
import parallelai.spyglass.hbase.{HBaseSource, HBasePipeConversions}
import cascading.tuple.Fields
import parallelai.spyglass.hbase.HBaseConstants.SourceMode

/**
 * Before executing this example, have a working HBase, and
 *
 * $ hbase shell
 *
 * hbase(main):003:0> create 'spyglass.hbase.test1' , 'data'
 * hbase(main):006:0> put 'spyglass.hbase.test1' , 'row1' , 'data:column1' , 'value1'
 * hbase(main):007:0> put 'spyglass.hbase.test1' , 'row2' , 'data:column1' , 'value2'
 * hbase(main):008:0> put 'spyglass.hbase.test1' , 'row3' , 'data:column1' , 'value3'
 * hbase(main):009:0> scan 'spyglass.hbase.test1'
 *
 */
class HBaseScanExample(args: Args) extends Job(args) with HBasePipeConversions {

  val SCHEMA = List('key, 'column1)
  val tableName = "spyglass.hbase.test1"
  val hbaseHost = "my-hbase-zookeeper-server:2181"

  val data = new HBaseSource(
    tableName,
    hbaseHost,
    SCHEMA.head,
    SCHEMA.tail.map((x: Symbol) => "data"),
    SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
    sourceMode = SourceMode.SCAN_ALL)
    .read
    .fromBytesWritable(SCHEMA)
    .debug
    .write(TSV("data/hbase.tsv"), writeHeader=true)


  }
