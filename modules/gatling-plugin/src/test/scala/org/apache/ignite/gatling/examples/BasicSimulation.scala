package org.apache.ignite.gatling.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import org.apache.ignite.gatling.Predef.allRecordsCheck
//import io.gatling.core.session.StaticValueExpression
//import io.gatling.core.session._
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.{ClientConfiguration, IgniteConfiguration}
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.IgnitionEx

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class BasicSimulation extends Simulation {
//  val protocol: IgniteProtocol = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))

  val c = new AtomicInteger(0)
  val feeder: Feeder[Int] = Iterator.continually(Map(
    "key" -> c.incrementAndGet(),
    "value" -> c.incrementAndGet()))

  val scn: ScenarioBuilder = scenario("Basic")
    .feed(feeder)
    .exec(ignite("Start client").start)
    .exec(ignite("Create cache").create("TEST-CACHE").backups(1))
    .exec(ignite("Put").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
    .exec(ignite("Get absent")
      .cache("TEST-CACHE")
      .get[Int, Int](100000)
      .check(
        simpleCheck(m => {
//          printf("m=%s\n", m.toString)
          m.isEmpty
        })
    ))
//    .exec { session => println(session); session }
    .exec(ignite("Get present")
      .cache("TEST-CACHE")
      .get[Int, Int]("#{key}")
//      .check(allResults[Int, Int].
      .check(
//        simpleCheck((m, s) => m.contains(s("key").as[Int])),
//        simpleCheck(m => m.contains(3)),
        allResults[Int, Int].saveAs("R"),
//        allResults[Int, Int].name("aaa"),
//        allRecordsCheck(responseTimeInMillis.lt(1))
      )

    )
    .exec { session => println(session); session }
    .exec(ignite("Close client").close)

  before(Ignition.start())
  after(Ignition.allGrids().get(0).close())

  setUp(
    scn
      .inject(
        constantUsersPerSec(2) during 1,
        nothingFor(1),
        constantUsersPerSec(4) during 1,
        incrementUsersPerSec(1).times(2).eachLevelLasting(1)
      )
  )
  .protocols(ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800")))
  .maxDuration(600.seconds)
}
