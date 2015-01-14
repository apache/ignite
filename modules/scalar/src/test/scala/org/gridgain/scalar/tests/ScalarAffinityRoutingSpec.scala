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

package org.gridgain.scalar.tests

import org.scalatest.matchers._
import org.scalatest._
import junit.JUnitRunner
import org.gridgain.scalar.scalar
import scalar._
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith

/**
 * Tests for `affinityRun..` and `affinityCall..` methods.
 */
@RunWith(classOf[JUnitRunner])
class ScalarAffinityRoutingSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
    /** Cache name. */
    private val CACHE_NAME = "partitioned_tx"

    "affinityRun$ method" should "run correctly" in scalar("examples/config/example-cache.xml") {
        val c = cache$[Int, Int](CACHE_NAME).get

        c += (0 -> 0)
        c += (1 -> 1)
        c += (2 -> 2)

        val cnt = c.dataStructures().atomicLong("affinityRun", 0, true)

        grid$.affinityRun$(CACHE_NAME, 0, () => { cnt.incrementAndGet() }, null)
        grid$.affinityRun$(CACHE_NAME, 1, () => { cnt.incrementAndGet() }, null)
        grid$.affinityRun$(CACHE_NAME, 2, () => { cnt.incrementAndGet() }, null)

        assert(cnt.get === 3)
    }

    "affinityRunAsync$ method" should "run correctly" in scalar("examples/config/example-cache.xml") {
        val c = cache$[Int, Int](CACHE_NAME).get

        c += (0 -> 0)
        c += (1 -> 1)
        c += (2 -> 2)

        val cnt = c.dataStructures().atomicLong("affinityRunAsync", 0, true)

        grid$.affinityRunAsync$(CACHE_NAME, 0, () => { cnt.incrementAndGet() }, null).get
        grid$.affinityRunAsync$(CACHE_NAME, 1, () => { cnt.incrementAndGet() }, null).get
        grid$.affinityRunAsync$(CACHE_NAME, 2, () => { cnt.incrementAndGet() }, null).get

        assert(cnt.get === 3)
    }
}
