package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import scala.language.postfixOps

class SimulationExample extends Simulation {
  val protocol = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))
  val feeder = IntPairsFeeder()
  val scn = scenario("Example")
    .feed(feeder)
    .exec(ignite("Start client") start)
    .exec(ignite("Create cache") create "TEST-CACHE" backups 0 atomicity TRANSACTIONAL)
    .exec(ignite("txStart")      txStart (PESSIMISTIC, REPEATABLE_READ) timeout 100)
    .exec(ignite("Put")          cache  "TEST-CACHE" put[Int, Int]("#{key}", "#{value}"))
    .exec(ignite("Get")          cache  "TEST-CACHE" get[Int, Int](key = "#{key}"))
    .exec(ignite("Commit")       commit)
    .exec(ignite("Close client") close)
  setUp(scn.inject(constantUsersPerSec(50) during 30)).protocols(protocol)
}
