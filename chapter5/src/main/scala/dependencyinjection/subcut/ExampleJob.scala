package dependencyinjection.subcut

import com.twitter.scalding._
import dependencyinjection.{ExternalServiceImpl, ExternalService, ExampleSchema}

/**
 * Read first the 'External Operations' design pattern
 *
 * This is an example job that uses 'subcut' for Dependency Injection
 * https://github.com/dickwall/subcut
 */
class ExampleJob(args: Args) extends Job(args) {
    import ExampleSchema._
    import ExternalServiceWrapper._
    import NewSerializableBindingModule._

    implicit val bindingModule = newSerializableBindingModule { bindingModule =>
      import bindingModule._

      bind [ExternalService] toSingle new ExternalServiceImpl()
    }

    Tsv(args("input"), LOG_SCHEMA).read
      .addUserInfo
      .write( Tsv(args("output"), OUTPUT_SCHEMA) )
  }
