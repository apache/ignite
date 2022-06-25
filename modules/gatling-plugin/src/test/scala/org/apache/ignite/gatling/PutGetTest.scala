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

package org.apache.ignite.gatling

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.IgniteClientApi.nodeApi
import org.apache.ignite.gatling.IgniteClientApi.thinClient
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.simulation.IgniteSupport
import org.junit.Test

class PutGetTest extends AbstractGatlingTest {
  val cache = "TEST-CACHE"

  override protected def beforeTest(): Unit = {
    super.beforeTest()
    val ignite = grid(0)
    ignite.createCache(
      new CacheConfiguration[Int, Int]()
        .setName(cache)
        .setCacheMode(PARTITIONED)
        .setAtomicityMode(TRANSACTIONAL)
    )
  }

  @Test
  def thinClientTest(): Unit = runWith(thinClient) {
    "org.apache.ignite.gatling.PutGetSimulation"
  }

  @Test
  def thickClientTest(): Unit = runWith(nodeApi) {
    "org.apache.ignite.gatling.PutGetSimulation"
  }
}

class PutGetSimulation extends Simulation with IgniteSupport with StrictLogging {

  private val cache = "TEST-CACHE"
  private val minusTwo = -2

  private val scn = scenario("Basic")
    .feed(feeder)
    .exec(start as "start")
    .exec(create(cache) backups 1 atomicity ATOMIC mode PARTITIONED as "create")
    .exec(put[Int, Int](cache, "#{key}", "#{value}") as "put")
    .exec(
      get[Int, Any](cache, key = minusTwo)
        check allResults[Int, Any].transform(r => r(minusTwo)).isNull as "get absent"
    )
    .exec { session =>
      logger.info(session.toString)
      session
    }
    .exec(
      get[Int, Int](cache, key = "#{key}")
        check (simpleCheck((r, s) => r(s("key").as[Int]) == s("value").as[Int]),
        allResults[Int, Int].saveAs("savedInSession")) as "get present"
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
