package dependencyinjection

import com.twitter.scalding._
import cascading.pipe.Pipe

trait ExampleOperations extends FieldConversions with TupleConversions {
  // Dsl._ contains an implicit that converts Pipe to a RichPipe
  import Dsl._

  def self: Pipe

  def externalService: ExternalService

  def addUserInfo: Pipe = self.map('user ->('email, 'address))
    { userId: String => externalService.getUserInfo(userId)}
}