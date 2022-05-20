package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import scala.concurrent.duration.DurationInt

class SimulationTransactions extends Simulation with DucktapeIgniteSupport {

  val feeder: IntPairsFeeder = IntPairsFeeder()

  val commitTx: ChainBuilder =
    exec(ignite("txStart-1").txStart (PESSIMISTIC, REPEATABLE_READ) timeout 100)
      .exec(ignite("put-1").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("commit").commit)
      .exec(ignite("get after commit")
        .cache("TEST-CACHE")
        .get[Int, Int]("#{key}")
        .check(
          allResults[Int, Int].saveAs("C"),
          simpleCheck((m, session) => {
            m(session("key").as[Int]) == session("value").as[Int]
          })
        )
      )

  val rollbackTx: ChainBuilder =
    exec(ignite("txStart-2").txStart)
      .exec(ignite("put-2").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("rollback").rollback)
      .exec(pause(1.seconds))
      .exec(ignite("get after rollback")
        .cache("TEST-CACHE")
        .get[Int, Integer]("#{key}")
        .check(
          simpleCheck((m, session) => {
            m(session("key").as[Int]) == null
          }),
          allResults[Int, Integer].saveAs("R"),
        )
      )

  val scn: ScenarioBuilder = scenario("Get")
    .feed(feeder)
    .exec(
      ignite("Start client").start
    )
    .exec(
      ignite("Create cache").create("TEST-CACHE").backups(0).atomicity(TRANSACTIONAL)
    )
    .exec(rollbackTx)
    .exec(commitTx)
    .exec(
      ignite("Close client").close
    )

  setUp(scn.inject(atOnceUsers(2)))
    .protocols(ducktapeIgnite)
    .maxDuration(60.seconds)
    .assertions(global.failedRequests.count.is(0))
}
