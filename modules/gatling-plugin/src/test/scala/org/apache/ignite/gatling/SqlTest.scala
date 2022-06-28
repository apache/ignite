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

import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.simulation.IgniteSupport

/**
 * Tests SQL queries.
 */
class SqlTest extends AbstractGatlingTest {
  /** @inheritdoc */
  override val simulation: String = "org.apache.ignite.gatling.SqlSimulation"
}

/**
 * SQL simulation.
 */
class SqlSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "TEST-CACHE"
  private val keyExpression = (s: Session) => s("key").as[Long].success
  private val scn = scenario("Sql")
    .feed(feeder)
    .execIgnite(
      start as "Start client",
      create(cache).backups(1) as "Create cache",
      sql(cache, "CREATE TABLE City (id int primary key, name varchar, region varchar)") as "Create table",
      sql(cache, "INSERT INTO City(id, name, region) VALUES(?, ?, ?)").args("#{key}", _ => UUID.randomUUID().toString, "R") as "Insert",
      sql(cache, "SELECT * FROM City WHERE id = ?")
        .args(keyExpression)
        .check(
          simpleSqlCheck { (m, s) =>
            val id: Int = m.head.head.asInstanceOf[Int]
            id == s("key").as[Int]
          },
          allSqlResults.transform(r => r.head).saveAs("firstRow")
        ) as "Select"
    )
    .exec { session =>
      logger.info(session.toString)
      session
    }
    .exec(close as "Close client")

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
