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
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.api.node.IgniteNodeApi
import org.apache.ignite.gatling.api.thin.IgniteThinApi
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests operations with binary objects and low-level access to underlining Ignite API
 * for functionality not exposed via the DSL.
 */
class BinaryTest extends AbstractGatlingTest {
  /** Class name of simulation */
  val simulation: String = "org.apache.ignite.gatling.BinarySimulation"

  /** Runs simulation with thin client. */
  @Test
  def thinClient(): Unit = runWith(ThinClient)(simulation)

  /** Runs simulation with thick client. */
  @Test
  def thickClient(): Unit = runWith(NodeApi)(simulation)
}

/**
 */
class BinarySimulation extends Simulation with IgniteSupport with StrictLogging {
  private val scn = scenario("Binary")
    .feed(feeder)
    .ignite(
      create("bootstrap-cache"),
      sql(
        "bootstrap-cache",
        "CREATE TABLE City (id int primary key, name varchar, region varchar) WITH \"cache_name=s.city,value_type=s.city\""
      ),
      group("run outside of transaction")(fragment("s.city")),
      sql(
        "bootstrap-cache",
        "CREATE TABLE City2 (id int primary key, name varchar, region varchar) WITH \"atomicity=transactional,cache_name=s.city2,value_type=s.city2\""
      ),
      tx("run in transaction")(fragment("s.city2"))
    )

  private def fragment(cache: String) = ignite(
    // Put binary object
    put[Int, BinaryObject](
      cache,
      "#{key}",
      session =>
        session
          .binaryBuilder()(typeName = cache)
          .setField("id", session("key").as[Int])
          .setField("name", "Nsk")
          .setField("region", "Region")
          .build()
          .success
    ).keepBinary,

    // sql select with the affinity awareness
    sql(cache, "SELECT * FROM City WHERE id = ?")
      .args("#{key}")
      .partitions { session =>
        (session.igniteApi match {
          case IgniteNodeApi(ignite) => List(ignite.affinity(cache).partition(session("key").as[Int]))
          case IgniteThinApi(_)      => List.empty
        }).success
      }
      .check(
        resultSet.count.is(1),
        resultSet.transform(row => row(1)).is("Nsk")
      )
  )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
