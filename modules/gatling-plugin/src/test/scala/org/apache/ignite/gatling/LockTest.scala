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
import java.util.concurrent.locks.Lock

import javax.cache.processor.MutableEntry

import scala.concurrent.duration.DurationInt

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.utils.AbstractGatlingTest
import org.apache.ignite.gatling.utils.IgniteClientApi.NodeApi
import org.apache.ignite.gatling.utils.IgniteSupport
import org.junit.Test

/**
 * Tests that locks work as expected.
 */
class LockTest extends AbstractGatlingTest {
  /** Tests simulation with thick client. */
  @Test
  def thickClient(): Unit = runWith(NodeApi)(simulation = "org.apache.ignite.gatling.LockSimulation")
}

/**
 * Tests that locks work as expected and invoke accepts lambda.
 */
class LockSimulation extends Simulation with IgniteSupport with StrictLogging {
  private val cache = "TEST-CACHE"
  private val key = "1"
  private val value = new AtomicInteger(0)
  private val scn = scenario("LockedInvoke")
    .feed(Iterator.continually(Map("value" -> value.incrementAndGet())))
    .ignite(
      create(cache) atomicity TRANSACTIONAL,
      lock(cache, key)
        check entries[String, Lock].transform(_.value).saveAs("lock"),
      put[String, Int](cache, key, "#{value}"),
      invoke[String, Int, Unit](cache, key) { e: MutableEntry[String, Int] =>
        e.setValue(-e.getValue)
      },
      get[String, Int](cache, key)
        check (
          entries[String, Int].validate((e: Entry[String, Int], s: Session) => e.value == -s("value").as[Int]),
          entries[String, Int].transform(-_.value).is("#{value}")
        ),
      unlock(cache, "#{lock}"),
      close
    )

  setUp(scn.inject(rampUsers(20).during(1.second))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
