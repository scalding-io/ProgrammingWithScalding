package jdbc

import com.twitter.scalding._

/**
 * See com.twitter.scalding.jdbc.JDBCTap
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
   val dbuser = "root"//"admin"
   val dbpass = "alg0alg0"//"password"
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

  // To read from that table we can do
  val read_from_mysql = MySQLTableSource.read
    .write(Tsv("jdbc-output"))
}
