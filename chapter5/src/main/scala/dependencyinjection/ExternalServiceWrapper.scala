package dependencyinjection

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

/**
 * Dependency is injected by the constructor
 */
object ExternalServiceWrapper {

    implicit class ExampleServiceWrapper(val self: Pipe)(implicit val externalService : ExternalService) extends ExampleOperations with Serializable
    implicit def fromRichPipe(rp: RichPipe)(implicit externalService : ExternalService) = new ExampleServiceWrapper(rp.pipe)

}