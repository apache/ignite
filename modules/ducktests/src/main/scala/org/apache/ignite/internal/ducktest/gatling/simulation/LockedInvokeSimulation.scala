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

import java.util.concurrent.locks.Lock

import javax.cache.processor.MutableEntry

import scala.concurrent.duration.DurationInt

import io.gatling.core.Predef._
import io.gatling.core.Predef.rampUsersPerSec
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.api.node.IgniteNodeApi
import org.apache.ignite.internal.ducktest.gatling.utils.DucktapeIgniteSupport
import org.apache.ignite.internal.ducktest.gatling.utils.IntPairsFeeder

/**
 * Simulation for locked entry processor.
 */
class LockedInvokeSimulation extends Simulation with DucktapeIgniteSupport {
  private val feeder: IntPairsFeeder = new IntPairsFeeder()
  private val scn: ScenarioBuilder = scenario("Get")
    .feed(feeder)
    .doIfOrElse(session => session.igniteApi.isInstanceOf[IgniteNodeApi])(
      ignite(
        start,
        create("TEST-CACHE").backups(1).atomicity(TRANSACTIONAL),
        lock[Int]("TEST-CACHE", "#{key}")
          check entries[Int, Lock].transform(_.value).saveAs("lock"),
        put[Int, Int]("TEST-CACHE", "#{key}", "#{value}"),
        invoke[Int, Int, Unit]("TEST-CACHE", "#{key}") { e: MutableEntry[Int, Int] =>
          e.setValue(-e.getValue)
        },
        get[Int, Int]("TEST-CACHE", "#{key}")
          check entries[Int, Int].transform(e => -e.value).is("#{value}"),
        unlock("TEST-CACHE", "#{lock}"),
        close
      )
    )(
      pause(100.milliseconds)
    )

  setUp(
    scn.inject(
      rampUsersPerSec(0).to(100).during(15.seconds),
      constantUsersPerSec(100) during 15
    )
  )
    .protocols(protocol)
    .assertions(global.failedRequests.count.is(0))
}
