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
package org.apache.ignite.internal.ducktest.gatling.simulation

import scala.concurrent.duration.DurationInt

import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.ducktest.gatling.utils.DucktapeIgniteSupport
import org.apache.ignite.internal.ducktest.gatling.utils.IntPairsFeeder
import org.apache.ignite.internal.ducktest.gatling.utils.IntPairsFeeder

/**
 * Simulation with transactions.
 */
class SimulationTransactions extends Simulation with DucktapeIgniteSupport {

  private val feeder: IntPairsFeeder = new IntPairsFeeder()

  private val commitTx: ChainBuilder = exec(
    tx(PESSIMISTIC, REPEATABLE_READ) /* timeout 100L */ (
      put("TEST-CACHE", "#{key}", "#{value}") as "put-commit",
      commit
    )
  ).exec(
    get[Int, Int]("TEST-CACHE", "#{key}")
      .check(
        entries[Int, Int].findAll.saveAs("C"),
        entries[Int, Int].transform(_.value).is("#{value}")
      )
  )

  private val rollbackTx: ChainBuilder = exec(
    tx(
      put("TEST-CACHE", "#{key}", "#{value}"),
      rollback
    )
  ).exec(
    get[Int, Int]("TEST-CACHE", "#{key}")
      .check(
        entries[Int, Int].findAll.saveAs("R"),
        entries[Int, Int].count.is(0)
      )
  )

  private val scn: ScenarioBuilder = scenario("Get")
    .feed(feeder)
    .exec(
      start
    )
//    .exec(
//      ignite("Create cache").create("TEST-CACHE").backups(0).atomicity(TRANSACTIONAL)
//    )
    .exec(rollbackTx)
    .exec(commitTx)
    .exec(
      close
    )

  setUp(
    scn.inject(
      atOnceUsers(1)
//    rampUsersPerSec(0).to(100).during(10.seconds),
//    constantUsersPerSec(100) during 30,
    )
  )
    .protocols(protocol)
    .maxDuration(60.seconds)
    .assertions(global.failedRequests.count.is(0))
}
