package org.apache.ignite.gatling.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ChainBuilder
import org.apache.ignite.gatling.Predef.allResults
import org.apache.ignite.gatling.protocol.IgniteThinProtocolBuilder

//import io.gatling.core.session.StaticValueExpression
//import io.gatling.core.session._
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class TransactionSimulation extends Simulation {
  val c = new AtomicInteger(0)
  val feeder: Feeder[Int] = Iterator.continually(Map(
    "key" -> c.incrementAndGet(),
    "value" -> c.incrementAndGet()))


  val tx: ChainBuilder = exec(ignite("Put").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
    .exec(ignite("Get").cache("TEST-CACHE").get[Int, Any]("#{key}"))

  val commitTx: ChainBuilder =
    exec(ignite("txStart-commit").txStart (PESSIMISTIC, READ_COMMITTED) timeout 3000L txSize 8)
      .exec(ignite("put-commit").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("commit").commit)
      .exec(ignite("get-commit")
        .cache("TEST-CACHE")
        .get[Int, Any]("#{key}")
        .check(
          allResults[Int, Any].saveAs("C"),
          simpleCheck((m, session) => {
            m(session("key").as[Int]) == session("value").as[Int]
          })
        )
      )
      .exec { session => println(session); session }

  val rollbackTx: ChainBuilder =
    exec(ignite("txStart-rollback").txStart (OPTIMISTIC, REPEATABLE_READ))
      .exec(ignite("put-rollback").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("rollback").rollback)
      .exec(ignite("get-rollback")
        .cache("TEST-CACHE")
        .get[Int, Any]("#{key}")
        .check(
          allResults[Int, Any].saveAs("R"),
          simpleCheck((m, session) => {
            println(m)
            m(session("key").as[Int]) == null
          }),
        )
      )
      .exec { session => println(session); session }

  val scn: ScenarioBuilder = scenario("Basic")
    .feed(feeder)
    .exec(ignite("Start client").start)
    .exec(ignite("Create cache").create("TEST-CACHE").backups(0).atomicity(TRANSACTIONAL))
    .exec(rollbackTx)
    .exec(commitTx)
    .exec { session =>
        println(session)
        session
    }
    .exec(ignite("Close client").close)

  before {
    Ignition.start()
  }
  after {
    Ignition.allGrids().get(0).close()
  }

  val protocol: IgniteThinProtocolBuilder = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))

  setUp(
    scn.inject(
      rampUsersPerSec(0).to(100).during(5.seconds),
      constantUsersPerSec(100) during 10,
    )
  )
  .protocols(protocol)
  .maxDuration(600.seconds)
}
