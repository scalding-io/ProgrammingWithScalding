package dependencyinjection

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

/**
 * Dependency is injected by the constructor
 */
object ExternalServiceWrapper {

  implicit class ExternalServiceWrapper(val self: Pipe)(implicit val externalService: ExternalService) extends ExampleOperations with Serializable

  implicit def fromRichPipe(richPipe: RichPipe)(implicit externalService: ExternalService) = new ExternalServiceWrapper(richPipe.pipe)(externalService)
}