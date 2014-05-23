package latebound

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

/**
 * Dependency is injected by the constructor
 */
object ExternalServiceWrapper {
  type ExternalServiceFactory = () => ExternalService
  implicit class ExternalServiceWrapper(val self: Pipe)(implicit externalServiceFactory : ExternalServiceFactory) extends ExampleOperations with Serializable {
    lazy val externalService : ExternalService = externalServiceFactory()
  }
  implicit def fromRichPipe(richPipe: RichPipe)(implicit externalServiceFactory : ExternalServiceFactory) = new ExternalServiceWrapper(richPipe.pipe)
}