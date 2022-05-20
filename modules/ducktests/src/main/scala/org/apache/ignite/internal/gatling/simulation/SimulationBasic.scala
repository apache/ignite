package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class SimulationBasic extends Simulation with DucktapeIgniteSupport {
    val protocol = ducktapeIgnite
    val feeder = IntPairsFeeder()

    val basicScenario = scenario("Basic")
      .feed(feeder)
      .exec(  ignite("start client")    start)
      .exec(  ignite("create cache 1")  create "TEST-CACHE" backups 1  atomicity ATOMIC mode PARTITIONED)
      .exec(  ignite("put 1")           cache  "TEST-CACHE"
                                        put[Int, Int] ("#{key}", "#{value}"))
      .exec( (ignite("get absent")      cache  "TEST-CACHE"
                                        get[Int, Int] (key = -2))
                                        check simpleCheck(result => result(-2) != null)
      )
      .exec( (ignite("get present")     cache("TEST-CACHE") get[Int, Int] (key = "#{key}"))
                                        check(
                                          simpleCheck((r, s) => r(s("key").as[Int]) == s("value").as[Int]),
                                          allResults[Int, Int].saveAs("savedInSession")
                                        )
      )
      .exec( ignite("close client")     close)

    setUp(
      basicScenario
        .inject(constantUsersPerSec(10) during 30.seconds))
        .protocols(protocol)
        .assertions(global.failedRequests.count.is(0))
}
