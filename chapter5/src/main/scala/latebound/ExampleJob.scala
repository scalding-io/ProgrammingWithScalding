package latebound

import com.twitter.scalding._

/**
 * Example job that uses Late Bound pattern
 */
class ExampleJob (args: Args) extends Job(args) {

  // import schema and wrapper
  import ExampleSchema._

  // Inject the dependency
  import LateBoundWrapper._
  implicit val externalServiceFactory = new ExternalServiceImpl()

  // Alternative way of injecting the dependency through a factory
  // import LateBoundFactoryWrapper._
  // implicit val externalServiceFactory : ExternalServiceFactory = () => new ExternalServiceImpl()

  // Construct pipeline using external operations
  Tsv(args("input"), LOG_SCHEMA).read
    .addUserInfo
    .debug
    .write(Tsv(args("output"), OUTPUT_SCHEMA))
}
