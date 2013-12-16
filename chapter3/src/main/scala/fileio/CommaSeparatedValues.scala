package fileio

import com.twitter.scalding._

/**
 * Example on how to read / write CSV files
 */
class CommaSeparatedValues(args: Args) extends Job(args) {

  val inputSchema = List('column1, 'column2, 'column3)
  // By defining a different separator,
  // we can load other type of Delimited files
  val separator = ","
  Csv( args("input"), separator, inputSchema)
    .read
    .debug
    .write( Csv( args("output") ))

}
