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

import io.gatling.core.Predef._
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.simulation.IgniteSupport

/**
 * Tests creation of all types of transactions.
 */
class TransactionTest extends AbstractGatlingTest {
  private val cache = "TEST-CACHE"
  /** @inheritdoc */
  override val simulation: String = "org.apache.ignite.gatling.TransactionSimulation"

  /** @inheritdoc */
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
}

/**
 * Simulation with all types of transactions.
 */
class TransactionSimulation extends Simulation with IgniteSupport {
  private val cache = "TEST-CACHE"
  private val scn = scenario("Basic")
    .feed(feeder)
    .execIgnite(
      start,
      tx(PESSIMISTIC, READ_COMMITTED).timeout(1000L)(
        put[Int, Int](cache, "#{key}", "#{value}"),
        get[Int, Any](cache, key = "#{key}") check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is("#{value}"),
        commit
      ),
      tx(PESSIMISTIC, READ_COMMITTED)
        .timeout(1000L)
        .txSize(8)(
          put[Int, Int](cache, 3, 4),
          get[Int, Any](cache, key = 3) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(4),
          commit
        ),
      tx(PESSIMISTIC, READ_COMMITTED)(
        put[Int, Int](cache, 1, 2),
        get[Int, Any](cache, key = 1) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(2),
        commit
      ),
      tx(
        put[Int, Int](cache, 1, 2),
        get[Int, Any](cache, key = 1) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(2),
        commit
      ),
      tx("tx1")(PESSIMISTIC, READ_COMMITTED).timeout(1000L)(
        put[Int, Int](cache, 1, 2),
        get[Int, Any](cache, key = 1) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(2),
        commit
      ),
      tx("tx2")(PESSIMISTIC, READ_COMMITTED)
        .timeout(1000L)
        .txSize(8)(
          put[Int, Int](cache, 1, 2),
          get[Int, Any](cache, key = 1) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(2),
          commit as "commit"
        ),
      tx("tx3")(PESSIMISTIC, READ_COMMITTED)(
        put[Int, Int](cache, 1, 2),
        get[Int, Any](cache, key = 1) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(2),
        rollback as "rollback"
      ),
      tx("tx4")(
        put[Int, Int](cache, 1, 2),
        get[Int, Any](cache, key = 1) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(2)
      ),
      get[Int, Any](cache, key = 1) check allResults[Int, Any].transform(r => r.values.head.asInstanceOf[Int]).is(2),
      close
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
