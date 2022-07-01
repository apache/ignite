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

import scala.language.postfixOps

import io.gatling.core.Predef._
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.ducktest.gatling.utils.IntPairsFeeder

/**
 */
class SimulationExample extends Simulation {
  private val protocol = igniteProtocol.cfg(new ClientConfiguration().setAddresses("localhost:10800"))
  private val feeder = new IntPairsFeeder()
  private val scn = scenario("Example")
    .feed(feeder)
    .ignite(
      start as "Start client",
      create("TEST-CACHE") backups 0 atomicity TRANSACTIONAL as "Create cache",
      tx(PESSIMISTIC, REPEATABLE_READ).timeout(100L)(
        put("TEST-CACHE", "#{key}", "#{value}") as "Put",
        get("TEST-CACHE", key = "#{key}") as "Get",
        commit as "Commit"
      ),
      close as "Close client"
    )
  setUp(scn.inject(constantUsersPerSec(50) during 30)).protocols(protocol)
}
