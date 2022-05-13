package org.apache.ignite.gatling.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.IgnitionEx

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class BasicSimulation extends Simulation {

    val cfg: ClientConfiguration = Option(System.getProperty("config"))
        .map(cfgPath => IgnitionEx.loadSpringBean[ClientConfiguration](cfgPath, "thin.client.cfg"))
        .getOrElse(new ClientConfiguration().setAddresses("localhost:10800"))

    val igniteP: IgniteProtocol = ignite.cfg(cfg)

    val c = new AtomicInteger(0)
    val feeder: Feeder[Int] = Iterator.continually(Map(
        "key" -> c.incrementAndGet(),
        "value" -> c.incrementAndGet()))

    val scn: ScenarioBuilder = scenario("Basic")
        .feed(feeder)
        .exec(ignite("Start client").startClient)
        .exec(ignite("Create cache").create("TEST-CACHE"))
        .exec(ignite("Put").cache("TEST-CACHE").put[Integer, Integer]("#{key}", "#{value}"))
        .exec(ignite("Close client").closeClient)

    setUp(scn
        .inject(
            constantUsersPerSec(50) during 5,
            nothingFor(5),
            constantUsersPerSec(100) during 5,
            incrementUsersPerSec(10).times(10).eachLevelLasting(10))
        )
        .protocols(igniteP)
        .maxDuration(600.seconds)
}
