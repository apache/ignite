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
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteClientApi.ThinClient
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 */
class TransactionTest extends AbstractGatlingTest {
  /** Class name of simulation */
  val simulation: String = "org.apache.ignite.gatling.TransactionSimulation"

  /** Runs simulation with thin client. */
  @Test
  def thinClient(): Unit = runWith(ThinClient)(simulation)

  /** Runs simulation with thick client. */
  @Test
  def thickClient(): Unit = runWith(NodeApi)(simulation)
}

/**
 * Commit and rollback simulation.
 */
class TransactionSimulation extends Simulation with StrictLogging with IgniteSupport {
  private val key = "key"
  private val value = "value"
  private val cache = "TEST-CACHE"

  private val commitTx = ignite(
    tx("commit transaction")(PESSIMISTIC, READ_COMMITTED)
      .timeout(3000L)
      .txSize(1)(
        get[Int, Int](cache, s"#{$key}") check entries[Int, Int].notExists,
        put[Int, Int](cache, s"#{$key}", s"#{$value}"),
        commit as "commit"
      ),
    get[Int, Any](cache, s"#{$key}") check (
      mapResult[Int, Any].saveAs("C"),
      mapResult[Int, Any].validate((m: Map[Int, Any], session: Session) => m(session(key).as[Int]) == session(value).as[Int])
    )
  )

  private val rollbackTx = ignite(
    tx(OPTIMISTIC, REPEATABLE_READ)(
      put[Int, Int](cache, s"#{$key}", s"#{$value}"),
      rollback as "rollback"
    ),
    get[Int, Any](cache, s"#{$key}") check (
      mapResult[Int, Any].saveAs("R"),
      mapResult[Int, Any].validate { (m: Map[Int, Any], session: Session) =>
        logger.info(m.toString)
        m(session(key).as[Int]) == null
      }
    )
  )

  private val autoRollbackTx = ignite(
    tx(
      put[Int, Int](cache, s"#{$key}", s"#{$value}")
    ),
    get[Int, Any](cache, s"#{$key}") check (
      mapResult[Int, Any].saveAs("R"),
      mapResult[Int, Any].validate { (m: Map[Int, Any], session: Session) =>
        logger.info(m.toString)
        m(session(key).as[Int]) == null
      }
    )
  )

  private val scn = scenario("Basic")
    .feed(feeder)
    .ignite(
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
