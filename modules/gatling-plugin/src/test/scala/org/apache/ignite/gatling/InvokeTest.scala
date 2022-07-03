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

import java.util.concurrent.atomic.AtomicInteger

import javax.cache.processor.MutableEntry

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.session.ExpressionSuccessWrapper
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests invoke entry processor.
 */
class InvokeTest extends AbstractGatlingTest {
  @Test
  /** Tests entry processor without arguments. */
  def noArgs(): Unit = runWith(NodeApi)(simulation = "org.apache.ignite.gatling.InvokeSimulation")

  @Test
  /** Tests entry processor with additional arguments passed. */
  def withArgs(): Unit = runWith(NodeApi)(simulation = "org.apache.ignite.gatling.InvokeArgsSimulation")
}

/**
 * invoke entry processor without arguments simulation.
 */
class InvokeSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "TEST-CACHE"
  private val key = "1"
  private val value = new AtomicInteger(0)
  private val scn = scenario("invoke")
    .feed(Iterator.continually(Map("value" -> value.incrementAndGet())))
    .ignite(
      create(cache),
      put[String, Int](cache, key, "#{value}"),
      invoke[String, Int, Unit](cache, key) { e: MutableEntry[String, Int] =>
        e.setValue(-e.getValue)
      },
      get[String, Int](cache, key)
        check (
          entries[String, Int].validate((e: Entry[String, Int], s: Session) => e.value == -s("value").as[Int]),
          entries[String, Int].transform(-_.value).is("#{value}")
        )
    )
  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}

/**
 * invoke entry processor with arguments simulation.
 */
class InvokeArgsSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "TEST-CACHE"
  private val key = "1"
  private val value = new AtomicInteger(0)

  private val fragment = ignite(
    put[String, Int](cache, key, "#{value}"),
    invoke[String, Int, Unit](cache, key).args(8.expressionSuccess) { (e: MutableEntry[String, Int], args: Seq[Any]) =>
      e.setValue(e.getValue * args.head.asInstanceOf[Integer])
    },
    get[String, Int](cache, key)
      check (
        entries[String, Int].validate((e: Entry[String, Int], s: Session) => e.value == 8 * s("value").as[Int]),
        entries[String, Int].transform(_.value / 8).is("#{value}")
      )
  )

  private val scn = scenario("invoke")
    .feed(Iterator.continually(Map("value" -> value.incrementAndGet())))
    .ignite(
      create(cache) atomicity TRANSACTIONAL,
      group("run outside of transaction")(fragment),
      tx("run in transaction")(fragment)
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
