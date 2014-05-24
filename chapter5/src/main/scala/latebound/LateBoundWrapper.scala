package latebound

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

/**
 * Dependency is injected lazily
 */
object LateBoundWrapper {
  implicit class LateBoundWrapper(val self: Pipe) extends ExampleOperations with Serializable {
    lazy val externalService = new ExternalServiceImpl
  }
  implicit def fromRichPipe(richPipe: RichPipe) = new LateBoundWrapper(richPipe.pipe)
}