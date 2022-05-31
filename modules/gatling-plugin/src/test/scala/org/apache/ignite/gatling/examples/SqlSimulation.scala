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

package org.apache.ignite.gatling.examples

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder

import java.util.UUID
import org.apache.ignite.gatling.protocol.IgniteThinProtocolBuilder

import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class SqlSimulation extends Simulation {
  val c = new AtomicInteger(0)
  val feeder: Feeder[Int] = Iterator.continually(Map(
    "key" -> c.incrementAndGet(),
    "value" -> c.incrementAndGet()))

  val scn: ScenarioBuilder = scenario("Basic")
    .feed(feeder)
    .exec(ignite("Start client").start)
    .exec(ignite("Create cache").create("T").backups(1))
    .exec(ignite("Create table").cache("T").sql(
      "CREATE TABLE City (id int primary key, name varchar, region varchar)"))
    .exec(ignite("Insert").cache("T").sql(
      "INSERT INTO City(id, name, region) VALUES(?, ?, ?)").args("#{key}", _ => UUID.randomUUID().toString, "R"))
    .exec(ignite("Select").cache("T").sql("SELECT * FROM City WHERE id = ?").args("#{key}")
      .check(
        simpleSqlCheck((m, s) => {
          val id : Int = m.head.head.asInstanceOf[Int]
          id == s("key").as[Int]
        }),
        allSqlResults.transform(r => r.head).saveAs("firstRow")
    ))
    .exec {
      session =>
        println(session)
        session }
    .exec(ignite("Close client").close)


  before {
    Ignition.start()
  }
  after {
    Ignition.allGrids().get(0).close()
  }

  val protocol: IgniteThinProtocolBuilder = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))

  setUp(
    scn
      .inject(
        rampUsers(1).during(5.seconds)
      )
  )
  .protocols(protocol)
  .maxDuration(600.seconds)
  .assertions(global.failedRequests.count.is(0))
}
