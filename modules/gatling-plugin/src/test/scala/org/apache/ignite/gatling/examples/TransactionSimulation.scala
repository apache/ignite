/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.gatling.examples

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ChainBuilder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.allResults
import org.apache.ignite.gatling.protocol.IgniteProtocol


class TransactionSimulation extends Simulation with StrictLogging {

  private val key = "key"
  private val value = "value"
  val c = new AtomicInteger(0)
  val feeder: Feeder[Int] = Iterator.continually(Map(
    key -> c.incrementAndGet(),
    value -> c.incrementAndGet()))


  private val cache = "TEST-CACHE"

  val tx1: ChainBuilder = exec(put[Int, Int](cache, s"#{$key}", s"#{$value}"))
    .exec(get[Int, Any](cache, s"#{$key}"))

  val commitTx: ChainBuilder =
    tx(PESSIMISTIC, READ_COMMITTED).timeout(3000L).txSize(8) (
      exec(put[Int, Int](cache, s"#{$key}", s"#{$value}"))
      .exec(commit)
      .exec(get[Int, Any](cache, s"#{$key}")
        .check(
          allResults[Int, Any].saveAs("C"),
          simpleCheck((m, session) => {
            m(session(key).as[Int]) == session(value).as[Int]
          })
        )
      )
      .exec { session => logger.info(session.toString); session }
    )

  val rollbackTx: ChainBuilder =
    tx(OPTIMISTIC, REPEATABLE_READ)(
      exec(put[Int, Int](cache, s"#{$key}", s"#{$value}"))
      .exec(rollback)
      .exec(get[Int, Any](cache, s"#{$key}")
        .check(
          allResults[Int, Any].saveAs("R"),
          simpleCheck((m, session) => {
            logger.info(m.toString)
            m(session(key).as[Int]) == null
          })
        )
      )
      .exec { session => logger.info(session.toString); session }
  )
  val scn: ScenarioBuilder = scenario("Basic")
    .feed(feeder)
    .exec(start)
    .exec(create(cache).backups(0).atomicity(TRANSACTIONAL))
    .exec(rollbackTx)
    .exec(commitTx)
    .exec { session =>
        logger.info(session.toString)
        session
    }
    .exec(close)

  before {
    Ignition.start()
  }
  after {
    Ignition.allGrids().get(0).close()
  }

  val protocol: IgniteProtocol = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))

  setUp(
    scn.inject(
      rampUsersPerSec(0).to(100).during(5.seconds),
      constantUsersPerSec(100) during 10
    )
  )
  .protocols(protocol)
  .maxDuration(600.seconds)
}
