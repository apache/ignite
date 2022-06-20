package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class SimulationBasic extends Simulation with DucktapeIgniteSupport {
    private val protocol = ducktapeIgnite
    private val feeder = IntPairsFeeder()

    private val basicScenario = scenario("Basic")
      .feed(feeder)
      .execIgnite(
        start,
        create("TEST-CACHE") backups 1  atomicity ATOMIC mode PARTITIONED,
        put[Int, Int] ("TEST-CACHE", "#{key}", "#{value}"),
        get[Int, Any] ("TEST-CACHE", key = -2)
          check simpleCheck(result => result(-2) != null) as "get absent",
        get[Int, Int] ("TEST-CACHE", key = "#{key}") check(
          simpleCheck((r, s) => r(s("key").as[Int]) == s("value").as[Int]),
          allResults[Int, Int].saveAs("savedInSession")) as "get present",
        create("TEST-CACHE-2") backups 1  atomicity ATOMIC mode PARTITIONED,
        put[Int, Any] ("TEST-CACHE-2", "#{key}", "#{savedInSession}"),
        close
      )

  private val twoScenario = scenario("Basic 2")
    .feed(feeder)
    .execIgnite(
      start,
      create("TEST-CACHE-2") backups 1  atomicity ATOMIC mode PARTITIONED,
      put[Int, Int] ("TEST-CACHE-2", "#{key}", "#{value}"),
      close
    )

  setUp(
      basicScenario
        .inject(constantUsersPerSec(10) during 30.seconds),
      twoScenario
        .inject(constantUsersPerSec(5) during 30.seconds)
  )
    .protocols(protocol)
    .assertions(global.failedRequests.count.is(0))
}
