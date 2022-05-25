package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef.{rampUsersPerSec, _}
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import scala.concurrent.duration.DurationInt

class SimulationTransactions extends Simulation with DucktapeIgniteSupport {

  val feeder: IntPairsFeeder = IntPairsFeeder()

  val commitTx: ChainBuilder =
    exec(ignite("txStart-commit").txStart (PESSIMISTIC, REPEATABLE_READ) timeout 100)
      .exec(ignite("put-commit").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("commit").commit)
      .exec(ignite("get-commit")
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
    exec(ignite("txStart-rollback").txStart)
      .exec(ignite("put-rollback").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("rollback").rollback)
      .exec(ignite("get-rollback")
        .cache("TEST-CACHE")
        .get[Int, Any]("#{key}")
        .check(
          allResults[Int, Any].saveAs("R"),
          simpleCheck((m, session) => {
            m(session("key").as[Int]) == null
          }),
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

  setUp(scn.inject(
//    atOnceUsers(2)
    rampUsersPerSec(0).to(100).during(10.seconds),
    constantUsersPerSec(100) during 30,
  ))
    .protocols(ducktapeIgnite)
    .maxDuration(60.seconds)
    .assertions(global.failedRequests.count.is(0))
}
