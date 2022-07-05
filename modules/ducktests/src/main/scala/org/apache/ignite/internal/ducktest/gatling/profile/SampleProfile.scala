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
package org.apache.ignite.internal.ducktest.gatling.profile

import scala.concurrent.duration.DurationInt
import scala.io.Source
import scala.util.Random
import scala.util.Using

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.entries
import org.apache.ignite.gatling.Predef.put
import org.apache.ignite.internal.ducktest.gatling.GatlingRunnerApplication

/**
 * Sample load testing profile.
 *
 * 1. Creates SQL table with integer primary key executing SQL statements from file script.
 *
 * 2. Fills about 30% of keys in 0..1000 interval as a test data.
 *
 * 3. Starts two scenarios in parallel:
 *
 * 3.1 80% - "read" scenario executing SQL query by indexed field.
 *
 * 3.2 20% - "write" scenario randomly inserting new or updating existing records for keys in the
 *     0..1000 interval.
 *
 * Profile assumes it can be used in distributed mode from several nodes. Node index (1-based)
 * and total number of client nodes involved are read from the system properties.
 */
class SampleProfile extends Profile {
  /** 1-based index of this node. */
  private val nodeIdx = Integer.valueOf(System.getProperty(GatlingRunnerApplication.NODE_IDX_PROPERTY_NAME, "1"))
  /** Total number of client nodes executing the simulation. */
  private val nodeCount = Integer.valueOf(System.getProperty(GatlingRunnerApplication.NODE_COUNT_PROPERTY_NAME, "1"))

  private val random = new Random(nodeIdx)

  /**
   * @return Scenario for caches creation.
   */
  override def ddl: ScenarioBuilder = scenario("Execute DDL script")
    .group("DDL")(
      doIfOrElse(nodeIdx == 1)(
        ignite(
          create("some"),
          foreach(ddlScript, "query")(
            sql("some", "#{query}") as "ddl"
          )
        )
      )(pause(10.milliseconds))
    )

  /**
   * @return Scenario for test data pre-load.
   */
  override def data: (Int, ScenarioBuilder) = (
    1000 / nodeCount / 3,
    scenario("Load test data")
      .feed(testDataFeeder)
      .group("Load test data")(insert as "load")
  )

  /**
   * Collection of load test scenarios to execute in parallel along with proportions.
   * @return Map from proportion to scenario.
   */
  override def test: Map[Double, ScenarioBuilder] = Map(
    20.0 -> putOrUpdateScenario,
    80.0 -> selectScenario
  )

  /** "Read" scenario. */
  private def selectScenario = scenario("Select")
    .feed(selectFeeder)
    .group("Select scenario")(
      sql("transaction", "SELECT * from transaction WHERE region = ?").args("#{region}") as "select"
    )

  /** "Write" scenario. */
  private def putOrUpdateScenario = scenario("Put")
    .feed(putOrUpdateFeeder)
    .group("Put or Update scenario")(
      tx(PESSIMISTIC, READ_COMMITTED)(
        get[Int, BinaryObject]("transaction", "#{id}")
          .check(
            entries[Int, BinaryObject].findAll.transform(_.nonEmpty).saveAs("exists"),
            entries[Int, BinaryObject].findAll.transform(_.headOption.map(_.value)).saveAs("savedObject")
          )
          .keepBinary as "get",
        doIfOrElse("#{exists}")(
          exec(update as "update")
        )(
          exec(insert as "insert")
        ),
        commit
      )
    )

  /** List of DDL statements read from the file script. */
  private val ddlScript =
    Using(Source.fromResource("sample-profile-ddl.sql")) { source =>
      source.getLines().toList
    }.getOrElse(List.empty)

  /** Node index aware feeder for testing data. */
  private val testDataFeeder: Feeder[Any] = Iterator.continually {
    Map(
      "id" -> random.between(1000 / nodeCount * (nodeIdx - 1), 1000 / nodeCount * nodeIdx),
      "region" -> s"Region${random.between(1, 20)}",
      "amount" -> random.between(1_000, 10_000_000)
    )
  }

  /** Feeder for "write" scenario. */
  private val putOrUpdateFeeder = Iterator.continually {
    Map(
      "id" -> random.nextInt(1000),
      "region" -> s"Region${random.between(1, 20)}",
      "amount" -> random.between(1_000, 10_000_000),
      "delta" -> random.between(100, 10_000)
    )
  }

  /** Feeder for "read" scenario. */
  private val selectFeeder = Iterator.continually {
    Map(
      "region" -> s"Region${random.between(1, 20)}"
    )
  }

  /** Helper for insert operation. */
  private val insert = put[Int, BinaryObject](
    "transaction",
    "#{id}",
    session =>
      session
        .binaryBuilder()(typeName = "transaction")
        .setField("id", session("id").as[Int])
        .setField("region", session("region").as[String])
        .setField("amount", session("amount").as[Int])
        .build()
        .success
  ).keepBinary as "insert"

  /**
   * Helper for update operation.
   * Assumes that copy of existing record was previously stored in session.
   */
  private val update = put[Int, BinaryObject](
    "transaction",
    "#{id}",
    session => {
      val savedObject = session("savedObject").as[Option[BinaryObject]].get
      savedObject.toBuilder
        .setField("amount", savedObject.field[Int]("amount") + session("delta").as[Int])
        .build()
        .success
    }
  ).keepBinary as "update"
}
