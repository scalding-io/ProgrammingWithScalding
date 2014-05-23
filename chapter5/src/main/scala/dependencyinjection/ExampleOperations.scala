package dependencyinjection

import com.twitter.scalding._
import cascading.pipe.Pipe

/**
 * This is exactly the same as in the 'Dependency Injection' example
 */
trait ExampleOperations extends FieldConversions with TupleConversions {
  // Dsl._ contains an implicit that converts Pipe to a RichPipe
  import Dsl._

  def self: Pipe

  def externalService: ExternalService

  // Joins with userData to add email and address
  def addUserInfo: Pipe = self.map('user ->('email, 'address))
    { userId: String => externalService.getUserInfo(userId)}
}