package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}
import org.apache.ignite.gatling.Predef._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class TransactionsSimulation extends IgniteSimulationBase {

  val c = new AtomicInteger(0)
  val feeder: Feeder[Int] = Iterator.continually(Map(
    "key" -> c.incrementAndGet(),
    "value" -> c.incrementAndGet()))

  val commitTx: ChainBuilder =
    exec(ignite("txStart-1").tx)
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
    exec(ignite("txStart-2").tx)
      .exec(ignite("put-2").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("rollback").rollback)
      .exec(pause(1.seconds))
      .exec(ignite("get after rollback")
        .cache("TEST-CACHE")
        .get[Int, Int]("#{key}")
        .check(
          simpleCheck((m, session) => {
            m(session("key").as[Int]) == null
          }),
          allResults[Int, Int].saveAs("R"),
        )
      )

  val scn: ScenarioBuilder = scenario("Basic")
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

  setUp(scn
    .inject(
      atOnceUsers(2)
      //  constantUsersPerSec(50) during 30,
      //            nothingFor(5),
      //            constantUsersPerSec(100) during 5,
      //            incrementUsersPerSec(10).times(5).eachLevelLasting(2)
    )
  )
    .protocols(protocol)
    .maxDuration(60.seconds)
    .assertions(global.failedRequests.count.is(0))
}
