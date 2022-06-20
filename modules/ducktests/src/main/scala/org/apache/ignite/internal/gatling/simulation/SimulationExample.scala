package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import scala.language.postfixOps

class SimulationExample extends Simulation {
  private val protocol = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))
  private val feeder = IntPairsFeeder()
  private val scn = scenario("Example")
    .feed(feeder)
    .execIgnite(
      start as "Start client",
      create("TEST-CACHE") backups 0 atomicity TRANSACTIONAL as "Create cache",
      tx(PESSIMISTIC, REPEATABLE_READ).timeout(100L) (
        put("TEST-CACHE", "#{key}", "#{value}") as "Put",
        get("TEST-CACHE", key = "#{key}") as "Get",
        commit as "Commit",
      ),
      close as "Close client"
    )
  setUp(scn.inject(constantUsersPerSec(50) during 30)).protocols(protocol)
}
