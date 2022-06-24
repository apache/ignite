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

package org.apache.ignite.internal.gatling.simulation

import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class SimulationBasic extends Simulation with DucktapeIgniteSupport {
//    private val protocol = ducktapeIgnite
    private val feeder = IntPairsFeeder()

    private val basicScenario = scenario("Basic")
      .feed(feeder)
      .execIgnite(
        start,
        create("TEST-CACHE") backups 1  atomicity ATOMIC mode PARTITIONED,
        put[Int, Int] ("TEST-CACHE", "#{key}", "#{value}"),
        get[Int, Any] ("TEST-CACHE", key = -2)
          check simpleCheck(result => result(-2) != null) as "get absent",
        get[Int, Int] ("TEST-CACHE", key = "#{key}") check(
          simpleCheck((r, s) => r(s("key").as[Int]) == s("value").as[Int]),
          allResults[Int, Int].saveAs("savedInSession")) as "get present",
        create("TEST-CACHE-2") backups 1  atomicity ATOMIC mode PARTITIONED,
        put[Int, Any] ("TEST-CACHE-2", "#{key}", "#{savedInSession}"),
        close
      )

  private val twoScenario = scenario("Basic 2")
    .feed(feeder)
    .execIgnite(
      start,
      create("TEST-CACHE-2") backups 1  atomicity ATOMIC mode PARTITIONED,
      put[Int, Int] ("TEST-CACHE-2", "#{key}", "#{value}"),
      close
    )

  setUp(
      basicScenario
        .inject(constantUsersPerSec(10) during 30.seconds),
      twoScenario
        .inject(constantUsersPerSec(5) during 30.seconds)
  )
    .protocols(protocol)
    .assertions(global.failedRequests.count.is(0))
}
