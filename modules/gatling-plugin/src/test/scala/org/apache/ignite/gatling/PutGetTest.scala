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
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests Put/Get/Remove key-value operations.
 */
class PutGetTest extends AbstractGatlingTest {
  /** Class name of simulation */
  val simulation: String = "org.apache.ignite.gatling.PutGetSimulation"

  /** Runs simulation with thin client. */
  @Test
  def thinClient(): Unit = runWith(ThinClient)(simulation)

  /** Runs simulation with thick client. */
  @Test
  def thickClient(): Unit = runWith(NodeApi)(simulation)
}

/**
 */
class PutGetSimulation extends Simulation with IgniteSupport with StrictLogging {

  private val scn = scenario("Basic")
    .feed(feeder)
    .ignite(
      create("TEST-CACHE-1") backups 1 atomicity ATOMIC mode PARTITIONED as "create",
      group("run outside of transaction")(fragment("TEST-CACHE-1")),
      create("TEST-CACHE-2") atomicity TRANSACTIONAL mode REPLICATED,
      tx("run in transaction")(fragment("TEST-CACHE-2"))
    )

  private def fragment(cache: String) = ignite(
    put[Int, Int](cache, _ => (100, 101).success) as "put100",
    get[Int, Int](cache, key = 100) check entries[Int, Int].count.is(1) as "get100",
    put[Int, Int](cache, "#{key}", "#{value}") as "put",
    get[Int, Any](cache, key = -2)
      check (
        mapResult[Int, Any].transform(r => r(-2)).isNull,
        entries[Int, Any].count.is(0),
        entries[Int, Any].notExists,
      ) as "get absent",
    get[Int, Int](cache, key = "#{key}")
      check (
        mapResult[Int, Int].saveAs("savedInSession"),
        mapResult[Int, Int].validate((m: Map[Int, Int], s: Session) => m(s("key").as[Int]) == s("value").as[Int]),
        entries[Int, Int].count.gt(0),
        entries[Int, Int].count.is(1),
        entries[Int, Int],
        entries[Int, Int].find,
        entries[Int, Int].find(0),
        entries[Int, Int].find(0).transform(_.value).is("#{value}"),
        entries[Int, Int].findAll,
        entries[Int, Int].is(s => s("key").validate[Int].flatMap(k => s("value").validate[Int].map(v => Entry(k, v)))),
        entries[Int, Int].is(Entry(1, 2))
      ) as "get present",
    remove[Int](cache, key = "#{key}"),
    getAndPut[Int, Int](cache, key = "#{key}", 1000)
      check (
        entries[Int, Int].count.is(0),
        entries[Int, Int].notExists
      ) as "getAndPut removed",
    getAndRemove[Int, Int](cache, key = "#{key}")
      check (
        entries[Int, Int].count.is(1),
        entries[Int, Int].exists,
        entries[Int, Int].transform(_.value).is(1000)
      ) as "getAndRemove",
    get[Int, Any](cache, key = -2)
      check (
        entries[Int, Any].count.is(0),
        entries[Int, Any].notExists,
      ) as "get removed"
  )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
