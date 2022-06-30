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
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.util.AbstractGatlingTest
import org.apache.ignite.gatling.util.IgniteSupport

/**
 */
class PutGetTest extends AbstractGatlingTest {
  /** @inheritdoc */
  override val simulation: String = "org.apache.ignite.gatling.PutGetSimulation"
}

/**
 */
class PutGetSimulation extends Simulation with IgniteSupport with StrictLogging {

  private val cache = "TEST-CACHE"
  private val minusTwo = -2

  private val scn = scenario("Basic")
    .feed(feeder)
    .ignite(
      start as "start",
      create(cache) backups 1 atomicity ATOMIC mode PARTITIONED as "create",
      put[Int, Int](cache, "#{key}", "#{value}") as "put",
      get[Int, Any](cache, key = minusTwo)
        check (
          mapResult[Int, Any].transform(r => r(minusTwo)).isNull,
          entries[Int, Any].count.is(0),
          entries[Int, Any].notExists,
        ) as "get absent"
    )
    .exec { session =>
      logger.info(session.toString)
      session
    }
    .ignite(
      get[Int, Int](cache, key = "#{key}")
        check (
          mapResult[Int, Int].saveAs("savedInSession"),
          mapResult[Int, Int].validate((m: Map[Int, Int], s: Session) => m(s("key").as[Int]) == s("value").as[Int]),
          entries[Int, Int].count.gt(0),
          entries[Int, Int].count.is(1),
          entries[Int, Int].exists,
          entries[Int, Int].find(0).transform(_.value).is("#{value}"),
          entries[Int, Int].is(s => s("key").validate[Int].flatMap(k => s("value").validate[Int].map(v => Entry(k, v)))),
          entries[Int, Int].is(Entry(1, 2))
        ) as "get present"
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
