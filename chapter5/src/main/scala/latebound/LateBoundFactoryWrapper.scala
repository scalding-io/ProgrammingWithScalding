package latebound

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

/**
 * Alternative way of providing a factory within our wrapper
 *
 * If we use -LateBoundFactoryWrapper- instead of the -LateBoundWrapper–
 * we instantiate with:
 *
 * implicit val externalServiceFactory : ExternalServiceFactory = () => new ExternalServiceImpl()
 */
object LateBoundFactoryWrapper {
  type ExternalServiceFactory = () => ExternalService
  implicit class LateBoundFactoryWrapper(val self: Pipe)(implicit externalServiceFactory : ExternalServiceFactory) extends ExampleOperations with Serializable {
    lazy val externalService : ExternalService = externalServiceFactory()
  }
  implicit def fromRichPipe(richPipe: RichPipe)(implicit externalServiceFactory : ExternalServiceFactory) = new LateBoundFactoryWrapper(richPipe.pipe)
}