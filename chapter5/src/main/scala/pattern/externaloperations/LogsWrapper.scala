package externaloperations

import com.twitter.scalding.RichPipe

object LogsWrapper {

  implicit def wrapPipe(self: cascading.pipe.Pipe): LogsWrapper = new LogsWrapper(new RichPipe(self))
  implicit class LogsWrapper(val self: RichPipe) extends LogsOperations with Serializable

}