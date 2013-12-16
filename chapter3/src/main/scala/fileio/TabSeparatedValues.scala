package fileio

import com.twitter.scalding._

/**
 * Example on how to read / write TSV files
 */
class TabSeparatedValues(args: Args) extends Job(args) {

  val inputSchema = List('column1, 'column2, 'column3)
  Tsv( args("input"), inputSchema)
    .read
    .debug
    .write( Tsv( args("output")))

}
