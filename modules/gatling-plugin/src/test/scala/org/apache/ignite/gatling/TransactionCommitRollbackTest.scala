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
import org.apache.ignite.gatling.simulation.IgniteSupport

/**
 *
 */
class TransactionCommitRollbackTest extends AbstractGatlingTest {
  /** @inheritdoc */
  override val simulation: String = "org.apache.ignite.gatling.TransactionCommitRollbackSimulation"
}

/**
 * Commit and rollback simulation.
 */
class TransactionCommitRollbackSimulation extends Simulation with StrictLogging with IgniteSupport {
  private val key = "key"
  private val value = "value"
  private val cache = "TEST-CACHE"

  private val commitTx = execIgnite(
    tx(PESSIMISTIC, READ_COMMITTED).timeout(3000L).txSize(1)(
      put[Int, Int](cache, s"#{$key}", s"#{$value}"),
      commit
    ),
    get[Int, Any](cache, s"#{$key}") check(
      allResults[Int, Any].saveAs("C"),
      simpleCheck((m, session) => m(session(key).as[Int]) == session(value).as[Int])
    )
  )

  private val rollbackTx = execIgnite(
    tx(OPTIMISTIC, REPEATABLE_READ)(
      put[Int, Int](cache, s"#{$key}", s"#{$value}"),
      rollback
    ),
    get[Int, Any](cache, s"#{$key}") check(
      allResults[Int, Any].saveAs("R"),
      simpleCheck { (m, session) =>
        logger.info(m.toString)
        m(session(key).as[Int]) == null
      }
    )
  )

  private val autoRollbackTx = execIgnite(
    tx(OPTIMISTIC, REPEATABLE_READ)(
      put[Int, Int](cache, s"#{$key}", s"#{$value}"),
    ),
    get[Int, Any](cache, s"#{$key}") check(
      allResults[Int, Any].saveAs("R"),
      simpleCheck { (m, session) =>
        logger.info(m.toString)
        m(session(key).as[Int]) == null
      }
    )
  )

  private val scn = scenario("Basic")
    .feed(feeder)
    .execIgnite(
      start,
      create(cache).backups(0).atomicity(TRANSACTIONAL),
      rollbackTx,
      autoRollbackTx,
      commitTx,
      exec { session =>
        logger.info(session.toString)
        session
      },
      close
    )

  setUp(scn.inject(atOnceUsers(10))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
