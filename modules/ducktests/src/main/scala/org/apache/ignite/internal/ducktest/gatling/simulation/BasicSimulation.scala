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
import scala.language.postfixOps

import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.ducktest.gatling.utils.DucktapeIgniteSupport
import org.apache.ignite.internal.ducktest.gatling.utils.IntPairsFeeder

/**
 * Simple gatling simulation.
 */
class BasicSimulation extends Simulation with DucktapeIgniteSupport {
  private val feeder = new IntPairsFeeder()

  private val basicScenario = scenario("Basic")
    .feed(feeder)
    .ignite(
      create("TEST-CACHE"),
      put[Int, Int]("TEST-CACHE", "#{key}", "#{value}"),
      get[Int, Int]("TEST-CACHE", key = -2)
        check entries[Int, Int].notExists as "get absent",
      get[Int, Int]("TEST-CACHE", key = "#{key}")
        check entries[Int, Int].find.transform(_.value).is("#{value}") as "get present"
    )

  setUp(
    basicScenario
      .inject(constantUsersPerSec(10) during 10.seconds)
  )
    .protocols(protocol)
    .assertions(global.failedRequests.count.is(0))
}
