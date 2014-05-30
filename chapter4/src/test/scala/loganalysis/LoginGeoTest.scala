package loganalysis

import com.twitter.scalding._
import org.scalatest._

//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//@RunWith(classOf[JUnitRunner])

class LoginGeoTest extends WordSpec with Matchers {
  // Import Dsl._ to get implicit conversions from List[Symbol] -> cascading.tuple.Fields etc
  import Dsl._

  val schema = List ('datetime, 'user, 'activity, 'data,
    'session, 'location, 'response, 'device, 'error)

  val testData = List(
    ("2014/07/01", "-", "login" , "-", "-" , "40.001,30.001" , "-", "PC", "-"),
    ("2014/07/01", "-", "login" , "-", "-" , "40.002,30.002" , "-", "PC", "-"))

  "The LoginGeo job" should {
      JobTest("loganalysis.LoginGeo")
        .arg("input", "inputFile")
        .arg("output", "outputFile")
        .source(Tsv("inputFile", schema), testData )
        .sink[(String)](Tsv("outputFile")){
           outputBuffer => val result = outputBuffer.mkString
           "identify nearby login events and bucket them" in {
             result shouldEqual s"""[{"lat":40.00,"lon":30.00,"device":"PC",count:2}]"""
           }
        }.run
         .finish
    }
}