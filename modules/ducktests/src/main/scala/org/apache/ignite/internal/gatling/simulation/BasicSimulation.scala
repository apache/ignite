package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.Predef._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class BasicSimulation extends IgniteSimulationBase {

    val c = new AtomicInteger(0)
    val feeder: Feeder[Int] = Iterator.continually(Map(
        "key" -> c.incrementAndGet(),
        "value" -> c.incrementAndGet()))

    val scn: ScenarioBuilder = scenario("Basic")
      .feed(feeder)
      .exec(
        ignite("Start client").start
      )
//      .exec(ignite("Create cache 1").create("TEST-CACHE-1").backups(1).atomicity(CacheAtomicityMode.TRANSACTIONAL))
//      .exec(ignite("Create cache 2 via thin api").create("TEST-CACHE-2").cfg(new ClientCacheConfiguration().setName("another-name")))
//      .exec(ignite("Create cache 3 via node api").create("TEST-CACHE-3").cfg(new CacheConfiguration[Integer, Integer]().setName("another-name")))
      .exec(
        ignite("Put") cache "TEST-CACHE" put[Integer, Integer]("#{key}", "#{value}")
      )
      .exec(ignite("Get absent")
        .cache("TEST-CACHE")
        .get[Int, Int](100000)
        .check(
          simpleCheck(m => {
            printf("m=%s\n", m.toString);
            m.isEmpty
          })
      ))
      .exec { session => println(session); session }
      .exec(ignite("Get present")
        .cache("TEST-CACHE")
        .get[Int, Int]("#{key}")
        .check(
          simpleCheck((m, s) => m.contains(s("key").as[Int])),
          simpleCheck(m => m.contains(3))
        )
      )
      .exec(
        ignite("Close client").close
      )

    setUp(scn
        .inject(
            constantUsersPerSec(50) during 30,
//            nothingFor(5),
//            constantUsersPerSec(100) during 5,
//            incrementUsersPerSec(10).times(5).eachLevelLasting(2)
          )
        )
        .protocols(protocol)
        .maxDuration(60.seconds)
        .assertions(global.failedRequests.count.is(0))
}
