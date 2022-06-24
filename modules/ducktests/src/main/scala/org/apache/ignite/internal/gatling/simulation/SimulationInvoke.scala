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

import io.gatling.core.Predef.{rampUsersPerSec, _}
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.internal.gatling.feeder.IntPairsFeeder

import java.util.concurrent.locks.Lock
import javax.cache.processor.MutableEntry
import scala.concurrent.duration.DurationInt

class SimulationInvoke extends Simulation with DucktapeIgniteSupport {

  val feeder: IntPairsFeeder = IntPairsFeeder()

  val scn: ScenarioBuilder = scenario("Get")
    .feed(feeder)
    .execIgnite(
      start,
      create("TEST-CACHE").backups(1).atomicity(TRANSACTIONAL),
      lock[Int]("TEST-CACHE", "#{key}").check(
        allResults[Int, Lock].transform(a => a.values.head).saveAs("lock")
      ),

      put[Int, Int]("TEST-CACHE", "#{key}", "#{value}"),
      invoke[Int, Int, Unit]("TEST-CACHE", "#{key}") {
        e: MutableEntry[Int, Int] => e.setValue(-e.getValue)
      },
      get[Int, Int]("TEST-CACHE", "#{key}")
        .check(
          simpleCheck((m, session) => {
            m(session("key").as[Int]) == -session("value").as[Int]
          }),
          allResults[Int, Int].transform(a => -a.values.head).is("#{value}")
        ),
      unlock("TEST-CACHE", "#{lock}"),

//      exec(session => {
//        val client: IgniteApi = session("igniteApi").as[IgniteApi]
//        val cache = client.cache[Int, Int]("TEST-CACHE").get
//        cache.get(session("key").as[Int])(
//          value => print(value)
//        )
//        session
//      }),
      close
    )

  setUp(scn.inject(
//    atOnceUsers(2)
    rampUsersPerSec(0).to(100).during(10.seconds),
    constantUsersPerSec(100) during 30,
  ))
    .protocols(protocol)
    .maxDuration(60.seconds)
    .assertions(global.failedRequests.count.is(0))
}
