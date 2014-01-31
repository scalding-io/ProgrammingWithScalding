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
import pattern.dependencyinjection.DependencyInjectedTransformations

object lateBindingTransformationsSchema {
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


trait LateBoundTransformations extends FieldConversions with TupleConversions {
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

object LateBindingTransformations {
  implicit class LateBindingTransformationsWrapper(val self: Pipe) extends LateBoundTransformations with Serializable {
    lazy val externalService = new ExternalServiceImpl
  }
  implicit def fromRichPipe(richPipe: RichPipe) = new LateBindingTransformationsWrapper(richPipe.pipe)
}

class LateBindingSampleJob(args: Args) extends Job(args) {
  import LateBindingTransformations._
  import lateBindingTransformationsSchema._

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addUserInfo
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )
}


object ConstructorLazilyInjectedTransformationsWrappers {
  type ExternalServiceFactory = () => ExternalService
  implicit class ConstructorLazilyInjectedTransformationsWrapper(val self: Pipe)(implicit externalServiceFactory : ExternalServiceFactory) extends LateBoundTransformations {
    lazy val externalService : ExternalService = externalServiceFactory()
  }
  implicit def fromRichPipe(richPipe: RichPipe)(implicit externalServiceFactory : ExternalServiceFactory) = new ConstructorLazilyInjectedTransformationsWrapper(richPipe.pipe)
}

class ConstructorInjectingSampleJob(args: Args) extends Job(args) {
  import ConstructorLazilyInjectedTransformationsWrappers._
  import lateBindingTransformationsSchema._

  implicit val externalServiceFactory : ExternalServiceFactory = () => new ExternalServiceImpl()

  Osv(args("eventsPath"), INPUT_SCHEMA).read
    .addUserInfo
    .write( Tsv(args("outputPath"), OUTPUT_SCHEMA) )
}

object FrameworkLazilyInjectedTransformationsWrappers {
  implicit class FrameworkInjectedTransformationsWrapper(val self: Pipe)(implicit val bindingModule : BindingModule) extends LateBoundTransformations with Injectable with Serializable {
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
  import lateBindingTransformationsSchema._
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