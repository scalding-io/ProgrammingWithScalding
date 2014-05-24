package latebound.subcut

import cascading.pipe.Pipe
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.twitter.scalding.RichPipe
import latebound.{ExampleOperations, ExternalService}

/**
 * In this example we are demonstrating how to use the framework 'subcut'
 *  to make a dependency injection
 */
object LateBoundSubcutWrapper {
  implicit def wrapPipe(richPipe: RichPipe)(implicit bindingModule: BindingModule) = new LateBoundSubcutWrapper(richPipe.pipe)

  implicit class LateBoundSubcutWrapper(val self: Pipe)(implicit val bindingModule: BindingModule) extends ExampleOperations with Injectable with Serializable {
    lazy val externalService = inject[ExternalService]
  }
}