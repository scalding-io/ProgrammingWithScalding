package latebound.subcut

import cascading.pipe.Pipe
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.twitter.scalding.RichPipe
import latebound.{ExampleOperations, ExternalService}

/**
 * In this example we are demonstrating how to use the framework 'subcut'
 *  to make a dependency injection
 */
object ExternalServiceWrapper {
  implicit def wrapPipe(richPipe: RichPipe)(implicit bindingModule: BindingModule) = new ExternalServiceWrapper(richPipe.pipe)

  implicit class ExternalServiceWrapper(val self: Pipe)(implicit val bindingModule: BindingModule) extends ExampleOperations with Injectable with Serializable {
    lazy val externalService = inject[ExternalService]
  }
}