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

package org.apache.ignite.gatling.action.cache

import java.util.UUID

import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._

class SqlSimulation extends IgniteTestSimulation {

  val cache = "TEST-CACHE"

  private val scn = scenario("Sql")
    .feed(feeder)
    .exec(ignite("Start client").start)
    .exec(ignite("Create cache").create(cache).backups(1))
    .exec(ignite("Create table").cache(cache).sql(
      "CREATE TABLE City (id int primary key, name varchar, region varchar)"))
    .exec(ignite("Insert").cache(cache).sql(
      "INSERT INTO City(id, name, region) VALUES(?, ?, ?)").args("#{key}", _ => UUID.randomUUID().toString, "R"))
    .exec(ignite("Select").cache(cache).sql("SELECT * FROM City WHERE id = ?").args("#{key}")
      .check(
        simpleSqlCheck((m, s) => {
          val id : Int = m.head.head.asInstanceOf[Int]
          id == s("key").as[Int]
        }),
        allSqlResults.transform(r => r.head).saveAs("firstRow")
      ))
    .exec {
      session =>
        logger.info(session.toString)
        session }
    .exec(ignite("Close client").close)

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
