package latebound.subcut

import com.escalatesoft.subcut.inject.{BindingModule, MutableBindingModule}

/**
 * NewBindingModule extends Serializable to allow class transfer
 * during MR execution
 */
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