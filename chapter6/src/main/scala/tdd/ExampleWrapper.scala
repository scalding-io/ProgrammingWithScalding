package tdd

import com.twitter.scalding.RichPipe
import cascading.pipe.Pipe

object ExampleWrapper {

  implicit class ExampleWrapper(val pipe: Pipe) extends ExampleOperations with Serializable
  implicit def wrapPipe(rp: RichPipe): ExampleWrapper = new ExampleWrapper(rp.pipe)

}