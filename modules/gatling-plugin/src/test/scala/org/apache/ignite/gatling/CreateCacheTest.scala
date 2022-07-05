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
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests various create cache operations.
 */
class CreateCacheTest extends AbstractGatlingTest {
  /** Create cache via thin client and simple DSL parameters. */
  @Test
  def thinClientDSL(): Unit = runWith(ThinClient)("org.apache.ignite.gatling.CreateCacheDSLSimulation")

  /** Create cache via thick client and simple DSL parameters. */
  @Test
  def thickClientDSL(): Unit = runWith(NodeApi)("org.apache.ignite.gatling.CreateCacheDSLSimulation")

  /** Create cache via thin client and full configuration instance. */
  @Test
  def thinClientConfig(): Unit = runWith(ThinClient)("org.apache.ignite.gatling.CreateCacheThinConfigSimulation")

  /** Create cache via thick client and full configuration instance. */
  @Test
  def thickClientConfig(): Unit = runWith(NodeApi)("org.apache.ignite.gatling.CreateCacheThickConfigSimulation")
}

/**
 * Create cache with simple parameters specified via DSL.
 */
class CreateCacheDSLSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "cache"
  private val scn = scenario("Basic")
    .feed(feeder)
    .ignite(
      create(s"$cache-1") as "1",
      create(s"$cache-2").backups(1) as "2",
      create(s"$cache-3").atomicity(TRANSACTIONAL) as "3",
      create(s"$cache-4").mode(PARTITIONED).backups(1) as "4",
      create(s"$cache-5").backups(1).atomicity(ATOMIC).mode(REPLICATED) as "5"
    )
  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}

/**
 * Create cache using full ClientCacheConfiguration instance.
 */
class CreateCacheThinConfigSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "cache-thin"
  private val scn = scenario("Basic")
    .feed(feeder)
    .ignite(create(cache).cfg(new ClientCacheConfiguration()))
  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}

/**
 * Create cache using full CacheConfiguration instance.
 */
class CreateCacheThickConfigSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "cache-thick"
  private val scn = scenario("Basic")
    .feed(feeder)
    .ignite(create(cache).cfg(new CacheConfiguration[Int, Int]()) as "create")
  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
