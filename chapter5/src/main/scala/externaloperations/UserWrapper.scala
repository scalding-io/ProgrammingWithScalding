package externaloperations

import cascading.pipe.Pipe
import com.twitter.scalding.RichPipe

object UserWrapper {

  implicit def wrapPipe2(self: Pipe): UserWrapper = new UserWrapper(new RichPipe(self))
  implicit class UserWrapper(val self: RichPipe) extends UserOperations //(with Serializable?)

}
