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
package org.apache.ignite.internal.ducktest.gatling.scenario

import scala.io.Source
import scala.util.Random

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.internal.ducktest.gatling.GatlingRunnerApplication
import org.apache.ignite.internal.ducktest.gatling.Profile

/**
 */
class PutGetProfile extends (() => Profile) {
  private val nodeIdx = Integer.valueOf(System.getProperty(GatlingRunnerApplication.NODE_IDX_PROPERTY_NAME, "0"))
  private val nodeCount = Integer.valueOf(System.getProperty(GatlingRunnerApplication.NODE_COUNT_PROPERTY_NAME, "0"))

  private val ddlFeeder: Feeder[Any] = csv("ddl.sql").queue()
  private val testDataFeeder: Feeder[Any] = Iterator.continually {
    Map(
      "key" -> Random.between(1000 / nodeCount * nodeIdx, 1000 / nodeCount * (nodeIdx + 1)),
      "value" -> Random.alphanumeric.take(50).toString()
    )
  }

  private val feeder: Feeder[Any] = Iterator.continually {
    Map(
      "key" -> Random.nextInt(1000),
      "value" -> Random.alphanumeric.take(50).toString()
    )
  }

  private val source = Source.fromResource("ddl.sql")
  private val ddl = source.getLines().toList
  source.close()

  /**
   * @return Scenario.
   */
  def apply(): Profile = (
    scenario("Execute DDL script")
      .feed(ddlFeeder)
      .ignite(
        doIf(nodeIdx == 0)(
          exec(create("some"))
            .exec(
              foreach(ddl, "query")(
                sql("some", "#{query}")
              )
            )
        )
      ),
    (
      100,
      scenario("Load test data")
        .feed(testDataFeeder)
        .ignite(
          create("TEST-CACHE") mode PARTITIONED atomicity ATOMIC backups 1 as "create",
          put[Int, String]("TEST-CACHE", "#{key}", "#{value}") as "load"
        )
    ),
    Map(
      0.2 -> scenario("Put")
        .feed(feeder)
        .group("put")(
          put[Int, String]("TEST-CACHE", "#{key}", "#{value}") as "put"
        ),
      0.8 -> scenario("Get")
        .feed(feeder)
        .group("get")(
          get[Int, String]("TEST-CACHE", "#{key}") as "get"
        )
    )
  )
}
