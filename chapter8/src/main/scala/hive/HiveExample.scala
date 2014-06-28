package hive

import cascading.tap.SinkMode
import com.twitter.scalding.{Job, Args, Tsv}
import io.scalding.taps.hive._

/**
 * HiveSource is actually a Hive file based tap.
 *
 * It actually queries the HCatalog to find out where data are stored,
 * how they are partitioned.
 *
 * It then creates a MultiFileInput source, based on the file format used
 * and reads data from the HDFS at maximum speed.
 *
 * We can use filters in our query to i.e. read all the partitions >= 20140701
 *
 * We are using https://github.com/scalding-io/scalding-taps
 */
class HiveExample (args: Args) extends Job(args) {

  val USER_SCHEMA = List('userId, 'username, 'photo)
  HiveSource("myHiveTable", SinkMode.KEEP)
    .withHCatScheme(osvInputScheme(fields = USER_SCHEMA))
    .write(Tsv("outputFromHive"))

}
