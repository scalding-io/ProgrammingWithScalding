package tdd

import com.twitter.scalding.RichPipe
import cascading.pipe.Pipe

object ExampleWrapper {

  implicit class ExampleOperationsWrapper(val pipe: Pipe) extends ExampleOperations with Serializable
  implicit def wrapPipe(rp: RichPipe): ExampleOperationsWrapper = new ExampleOperationsWrapper(rp.pipe)

}