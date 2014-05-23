package latebound

/**
 * This is an external service returning <UserInfo> objects
 */
trait ExternalService {
  def getUserInfo(userId: String): UserInfo
}

// NOTE: This class is NOT serializable
class ExternalServiceImpl extends ExternalService {
  def getUserInfo(userId: String): UserInfo = UserInfo("email", "address")
}

case class UserInfo(email: String, address: String)