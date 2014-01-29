package pattern.dependencyinjection

import cascading.pipe.Pipe
import com.twitter.scalding._
import com.twitter.scalding.Osv
import com.escalatesoft.subcut.inject.{MutableBindingModule, BindingModule, Injectable}

object dependencyInjectedTransformationsSchema {
  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val OUTPUT_SCHEMA = List('date, 'userid, 'url, 'email, 'address)
}

case class UserInfo(email: String, address: String)

trait ExternalService {
  def getUserInfo(userId: String) : UserInfo
}

// NOTE: This class is NOT serializable
class ExternalServiceImpl extends ExternalService {
  def getUserInfo(userId: String): UserInfo = UserInfo("email", "address")
}

trait DependencyInjectedTransformations extends FieldConversions with TupleConversions {
  import Dsl._

  def self: Pipe

  def externalService : ExternalService

  /** Joins with userData to add email and address
    *
    * Input schema: INPUT_SCHEMA
    * User data schema: USER_DATA_SCHEMA
    * Output schema: OUTPUT_SCHEMA */
  def addUserInfo : Pipe = self.map('userid -> ('email, 'address) ) { userId : String => externalService.getUserInfo(userId) }
}

object ConstructorInjectedTransformationsWrappers {
  implicit class ConstructorInjectedTransformationsWrapper(val self: Pipe)(implicit val externalService : ExternalService) extends DependencyInjectedTransformations with Serializable
  implicit def fromRichPipe(richPipe: RichPipe)(implicit externalService : ExternalService) = new ConstructorInjectedTransformationsWrapper(richPipe.pipe)
}

class ConstructorInjectingSampleJob(args: Args) extends Job(args) {
  import ConstructorInjectedTransformationsWrappers._
  import dependencyInjectedTransformationsSchema._

  implicit val externalServiceImpl = new ExternalServiceImpl()

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addUserInfo
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )
}

object FrameworkInjectedTransformationsWrappers {
  implicit class FrameworkInjectedTransformationsWrapper(val self: Pipe)(implicit val bindingModule : BindingModule) extends DependencyInjectedTransformations with Injectable with Serializable {
    lazy val externalService = inject[ExternalService]
  }
  implicit def fromRichPipe(richPipe: RichPipe)(implicit bindingModule : BindingModule) = new FrameworkInjectedTransformationsWrapper(richPipe.pipe)
}

// Need a NewBindingModule extending Serializable to allow the class to be trasferred during MR execution
class NewSerializableBindingModule(fn: MutableBindingModule => Unit) extends BindingModule with Serializable {
  lazy val bindings = {
    val module = new Object with MutableBindingModule
    fn(module)
    module.freeze().fixed.bindings
  }
}

object NewSerializableBindingModule {
  def newSerializableBindingModule(function: (MutableBindingModule) => Unit) = new NewSerializableBindingModule(function)
}

class FrameworkInjectingSampleJob(args: Args) extends Job(args) {
  import FrameworkInjectedTransformationsWrappers._
  import dependencyInjectedTransformationsSchema._
  import NewSerializableBindingModule._

  implicit val bindingModule = newSerializableBindingModule { bindingModule =>
    import bindingModule._

    bind [ExternalService] toSingle new ExternalServiceImpl()
  }

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addUserInfo
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )
}