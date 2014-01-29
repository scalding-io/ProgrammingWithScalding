package unserializable

import cascading.pipe.Pipe
import com.escalatesoft.subcut.inject.NewBindingModule._
import com.twitter.scalding._
import com.twitter.scalding.Args
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.Job
import com.twitter.scalding.Osv
import com.escalatesoft.subcut.inject.{MutableBindingModule, BindingModule, Injectable}
import com.twitter.scalding.Osv
import com.twitter.scalding.RichPipe
import com.twitter.scalding.Tsv
import com.twitter.scalding.TupleConversions

object dependencyInjectedTransformationsSchema {
  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val OUTPUT_SCHEMA = List('date, 'userid, 'url, 'email, 'address)
}

case class UserInfo(email: String, address: String)

trait ExternalService {
  def getUserInfo(userId: String) : UserInfo
}

class ExternalServiceImpl extends ExternalService {
  def getUserInfo(userId: String): UserInfo = ??? //Calls an external web service
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

object ConstructorLazilyInjectedTransformationsWrappers {
  type ExternalServiceFactory = () => ExternalService
  implicit class ConstructorLazilyInjectedTransformationsWrapper(val self: Pipe)(implicit externalServiceFactory : ExternalServiceFactory) extends DependencyInjectedTransformations {
    lazy val externalService : ExternalService = externalServiceFactory()
  }
  implicit def fromRichPipe(richPipe: RichPipe)(implicit externalServiceFactory : ExternalServiceFactory) = new ConstructorLazilyInjectedTransformationsWrapper(richPipe.pipe)
}

class ConstructorInjectingSampleJob(args: Args) extends Job(args) {
  import ConstructorLazilyInjectedTransformationsWrappers._
  import dependencyInjectedTransformationsSchema._

  implicit val externalServiceFactory : ExternalServiceFactory = () => new ExternalServiceImpl()

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addUserInfo
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )
}

object FrameworkLazilyInjectedTransformationsWrappers {
  implicit class FrameworkInjectedTransformationsWrapper(val self: Pipe)(implicit val bindingModule : BindingModule) extends DependencyInjectedTransformations with Injectable with Serializable {
    val externalService = inject[ExternalService]
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
  import FrameworkLazilyInjectedTransformationsWrappers._
  import dependencyInjectedTransformationsSchema._
  import NewSerializableBindingModule._

  implicit val bindingModule = newSerializableBindingModule { bindingModule =>
    import bindingModule._

    bind [ExternalService] toSingle externalServiceFactory
  }

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addUserInfo
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )

  def externalServiceFactory() = new ExternalServiceImpl()
}