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