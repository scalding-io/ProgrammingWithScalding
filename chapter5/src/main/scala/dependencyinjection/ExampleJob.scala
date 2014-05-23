package dependencyinjection

import com.twitter.scalding._

/**
 * Example job that uses Dependency Injection
 */
class ExampleJob(args: Args) extends Job(args) {

  // import schema and wrapper
  import ExampleSchema._
  import ExternalServiceWrapper._

  // Inject the dependency
  implicit val externalService = new ExternalServiceImpl()

  // Construct pipeline using external operations
  Tsv(args("input"), LOG_SCHEMA).read
    .addUserInfo
    .write(Tsv(args("output"), OUTPUT_SCHEMA))
}
