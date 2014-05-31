package jdbc

import com.twitter.scalding._
import com.twitter.scalding.jdbc._

/**
 * See com.twitter.scalding.jdbc.JDBCTap
 *
 * Before running this example code - make sure mysql is listening at port 3306
 * and that a database 'testdb' is already created i.e.
 *
 * mysql> CREATE DATABASE testdb;
 *
 * The table will be created for us by the Tap if it does not exist
 **/
class JDBCExample (args: Args) extends Job(args) {

  /**
   * Definition of JDBCSource
   */
  case object MySQLTableSource extends JDBCSource {
    override val tableName = "tableName"
    override val columns = List(
       varchar("user", 16),
       date("time"),
       varchar("activity",256),
       smallint("code")
   )

   val connectUrl = "jdbc:mysql://localhost:3306/testdb"
   val dbuser = "root"
   val dbpass = "password"
   val adapter= "mysql"
   override def currentConfig = ConnectionSpec(connectUrl,dbuser,dbpass,adapter)
  }

  /**
   * Scalding
   */
  val schema = List('user,'time, 'activity,'code)

  val inmemory_data = List(
    ("user1", "2014-06-15 10:15:30", "activity1", 1),
    ("user2", "2014-06-15 10:15:35", "activity2", 10),
    ("user3", "2014-06-15 10:15:38", "activity3", 1))

  // Read from List into Pipe and then store in MySQL
  val jdbc_pipe =
    IterableSource[(String, String, String, Int)](inmemory_data, schema )
    .read
    .write(MySQLTableSource)

  // Running this job multiple times, will generate multiple rows

  //To read from that table we can do
  val read_from_mysql = MySQLTableSource.read
    .write(Tsv("data"))
}
