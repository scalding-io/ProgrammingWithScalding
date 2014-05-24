package latebound

/**
 * This is an external service returning <UserInfo> objects
 */
trait ExternalService {
  def getUserInfo(userId: String): UserInfo
}

class ExternalServiceImpl extends ExternalService {
  def getUserInfo(userId: String): UserInfo = UserInfo("email", "address")
}

// NOTE: This class is NOT serializable
case class UserInfo(email: String, address: String)