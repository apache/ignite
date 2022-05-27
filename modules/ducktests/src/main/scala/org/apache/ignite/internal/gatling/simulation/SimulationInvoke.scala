package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef.{rampUsersPerSec, _}
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import java.util.concurrent.locks.Lock
import javax.cache.processor.MutableEntry
import scala.concurrent.duration.DurationInt

class SimulationInvoke extends Simulation with DucktapeIgniteSupport {

  val feeder: IntPairsFeeder = IntPairsFeeder()

  val scn: ScenarioBuilder = scenario("Get")
    .feed(feeder)
    .exec(
      ignite("Start client").start
    )
    .exec(
      ignite("Create cache").create("TEST-CACHE").backups(1).atomicity(TRANSACTIONAL)
    )
    .exec(ignite("lock").cache("TEST-CACHE")
      .lock[Int]("#{key}")
      .check(
        allResults[Int, Lock].transform(a => a.values.head).saveAs("lock")
      )
    )
    .exec(ignite("put").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
    .exec(ignite("invoke").cache("TEST-CACHE").invoke[Int, Int, Unit]("#{key}") {
      e: MutableEntry[Int, Int] => {
        e.setValue(-e.getValue)
      }
    })
    .exec(ignite("get")
      .cache("TEST-CACHE")
      .get[Int, Int]("#{key}")
      .check(
        simpleCheck((m, session) => {
          m(session("key").as[Int]) == -session("value").as[Int]
        }),
        allResults[Int, Int].transform(a => -a.values.head).is("#{value}")
      )
    )
    .exec(ignite("unlock").cache("TEST-CACHE").unlock("#{lock}"))
    .exec(
      ignite("Close client").close
    )

  setUp(scn.inject(
//    atOnceUsers(2)
    rampUsersPerSec(0).to(100).during(10.seconds),
    constantUsersPerSec(100) during 30,
  ))
    .protocols(ducktapeIgnite)
    .maxDuration(60.seconds)
    .assertions(global.failedRequests.count.is(0))
}
