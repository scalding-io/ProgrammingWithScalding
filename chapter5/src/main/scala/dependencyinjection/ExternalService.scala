package dependencyinjection

/**
 * This is an external service returning <UserInfo> in tuples
 */
trait ExternalService {
  def getUserInfo(userId: String): (String,String)
}

class ExternalServiceImpl extends ExternalService {
  def getUserInfo(userId: String) = ("email", "address")
}

// NOTE: This class is NOT serializable

//case class UserInfo(email: String, address: String)
