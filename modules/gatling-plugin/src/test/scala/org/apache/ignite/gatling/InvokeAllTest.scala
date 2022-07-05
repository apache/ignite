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

import javax.cache.processor.MutableEntry

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.session.ExpressionSuccessWrapper
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests invokeAll.
 */
class InvokeAllTest extends AbstractGatlingTest {
  @Test
  /** Tests simulation with the single entry processor passed to invokeAll. */
  def testSingleProcessor(): Unit = runWith(NodeApi)("org.apache.ignite.gatling.SingleProcessorSimulation")

  @Test
  /** Tests simulation with multiple entry processors passed to invokeAll. */
  def multipleProcessors(): Unit = runWith(NodeApi)("org.apache.ignite.gatling.MultipleProcessorsSimulation")
}

/**
 * invokeAll with single entry processor simulation.
 */
class SingleProcessorSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val scn = scenario("invokeAll")
    .feed(new BatchFeeder())
    .ignite(
      create("TEST-CACHE-1") backups 1 atomicity ATOMIC mode PARTITIONED as "create",
      group("run outside of transaction")(fragment("TEST-CACHE-1")),
      create("TEST-CACHE-2") atomicity TRANSACTIONAL mode REPLICATED,
      tx("run in transaction")(fragment("TEST-CACHE-2"))
    )

  private def fragment(cache: String) = ignite(
    putAll[Int, Int](cache, "#{batch}"),
    invokeAll[Int, Int, Unit](cache, Set(1, 3, 5).expressionSuccess) { e: MutableEntry[Int, Int] =>
      e.setValue(-e.getValue)
    },
    invokeAll[Int, Int, Unit](cache, Set(1, 3, 5).expressionSuccess).args(3.expressionSuccess) {
      (e: MutableEntry[Int, Int], args: Seq[Any]) =>
        e.setValue(e.getValue * args.head.asInstanceOf[Integer])
    },
    getAll[Int, Int](cache, Set(1, 3, 5))
      check
        entries[Int, Int].findAll
          .validate((entries: Seq[Entry[Int, Int]], _: Session) => entries.forall(e => e.value == -3 * (e.key + 1))) as "getAll"
  )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}

/** Entry processor with arguments. */
private class EntryProcessor extends CacheEntryProcessor[Int, Int, Unit]() {
  /**
   * @param e Cache entry.
   * @param args Arguments
   */
  override def process(e: MutableEntry[Int, Int], args: Object*): Unit =
    e.setValue(e.getValue * args.toList.head.asInstanceOf[Integer])
}

/**
 * invokeAll with multiple entry processors simulation.
 */
class MultipleProcessorsSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val scn = scenario("invokeAll")
    .feed(new BatchFeeder())
    .ignite(
      create("TEST-CACHE-1") backups 1 atomicity ATOMIC mode PARTITIONED as "create",
      group("run outside of transaction")(fragment("TEST-CACHE-1")),
      create("TEST-CACHE-2") atomicity TRANSACTIONAL mode REPLICATED,
      tx("run in transaction")(fragment("TEST-CACHE-2"))
    )

  private def fragment(cache: String) = ignite(
    putAll[Int, Int](cache, "#{batch}"),
    invokeAll[Int, Int, Unit](
      cache,
      map = Map(
        1 -> new EntryProcessor(),
        3 -> new EntryProcessor()
      )
    ).args(2.expressionSuccess),
    getAll[Int, Int](cache, Set(1, 3))
      check
        entries[Int, Int].findAll
          .validate((entries: Seq[Entry[Int, Int]], _: Session) => entries.forall(e => e.value == 2 * (e.key + 1)))
  )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
