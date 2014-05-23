package latebound

import com.twitter.scalding._
import latebound.ExternalServiceWrapper.ExternalServiceFactory

/**
 * Example job that uses Late Bound pattern
 */
class ExampleJob (args: Args) extends Job(args) {

  // import schema and wrapper
  import ExampleSchema._
  import LateBoundWrapper._

  // Inject the dependency
  implicit val externalServiceFactory : ExternalServiceFactory = () => new ExternalServiceImpl()

  // Construct pipeline using external operations
  Tsv(args("input"), INPUT_SCHEMA).read
    .addUserInfo
    .write(Tsv(args("output"), OUTPUT_SCHEMA))
}
