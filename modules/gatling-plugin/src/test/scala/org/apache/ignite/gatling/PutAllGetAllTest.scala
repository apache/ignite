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
import io.gatling.core.session.ExpressionSuccessWrapper
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests PutAll/GetAll operations.
 */
class PutAllGetAllTest extends AbstractGatlingTest {
  /** Class name of simulation */
  val simulation: String = "org.apache.ignite.gatling.PutAllGetAllSimulation"

  /** Runs simulation with thin client. */
  @Test
  def thinClient(): Unit = runWith(ThinClient)(simulation)

  /** Runs simulation with thick client. */
  @Test
  def thickClient(): Unit = runWith(NodeApi)(simulation)
}

/**
 * PutAll/GetAll simulation.
 */
class PutAllGetAllSimulation extends Simulation with IgniteSupport with StrictLogging {

  private val scn = scenario("Basic")
    .feed(new BatchFeeder())
    .ignite(
      create("TEST-CACHE-1") backups 1 atomicity ATOMIC mode PARTITIONED as "create",
      group("run outside of transaction")(fragment("TEST-CACHE-1")),
      create("TEST-CACHE-2") atomicity TRANSACTIONAL mode REPLICATED,
      tx("run in transaction")(fragment("TEST-CACHE-2"))
    )

  private def fragment(cache: String) = ignite(
    putAll[Int, Int](cache, "#{batch}") as "putAll from session",
    putAll[Int, Int](cache, Map(7 -> 8, 9 -> 10).expressionSuccess),
    getAll[Int, Int](cache, keys = Set(1, 7, 100))
      check (
        mapResult[Int, Int].transform(map => map.size).is(2),
        mapResult[Int, Int].transform(map => map(1)).is(2),
        mapResult[Int, Int].transform(map => map(7)).is(8),
        entries[Int, Int].count.is(2),
        entries[Int, Int].exists,
        entries[Int, Int].findRandom.validate((e: Entry[Int, Int], _: Session) => e.value == e.key + 1)
      ) as "getAll with one absent",
    getAll[Int, Int](cache, keys = Set(3, 5, 9))
      check (
        mapResult[Int, Int].transform(map => map.size).is(3),
        mapResult[Int, Int].transform(map => map(3)).is(4),
        mapResult[Int, Int].transform(map => map(9)),
        entries[Int, Int].count.is(3),
        entries[Int, Int].exists,
        entries[Int, Int].find(2).exists,
        entries[Int, Int].find(1)
      ) as "getAll present",
    removeAll[Int](cache, keys = Set(1, 9)),
    getAll[Int, Int](cache, keys = Set(1, 9))
      check (
        entries[Int, Int].count.is(0),
        entries[Int, Int].notExists,
      )
  )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
