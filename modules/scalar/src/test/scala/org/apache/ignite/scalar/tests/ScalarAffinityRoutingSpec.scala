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

package org.apache.ignite.scalar.tests

import org.apache.ignite.Ignition
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

/**
 * Tests for `affinityRun..` and `affinityCall..` methods.
 */
@RunWith(classOf[JUnitRunner])
class ScalarAffinityRoutingSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
    private val CFG = "modules/scalar/src/test/resources/spring-cache.xml"

    /** Cache name. */
    private val CACHE_NAME = "partitioned_tx"

    "affinityRun$ method" should "run correctly" in scalar(CFG) {
        val c = cache$[Int, Int](CACHE_NAME).get

//        c += (0 -> 0)
//        c += (1 -> 1)
//        c += (2 -> 2)

        val cnt = Ignition.ignite.atomicLong("affinityRun", 0, true)

        ignite$.affinityRun$(CACHE_NAME, 0, () => { cnt.incrementAndGet() }, null)
        ignite$.affinityRun$(CACHE_NAME, 1, () => { cnt.incrementAndGet() }, null)
        ignite$.affinityRun$(CACHE_NAME, 2, () => { cnt.incrementAndGet() }, null)

        assert(cnt.get === 3)
    }

    "affinityRunAsync$ method" should "run correctly" in scalar(CFG) {
        val c = cache$[Int, Int](CACHE_NAME).get

//        c += (0 -> 0)
//        c += (1 -> 1)
//        c += (2 -> 2)

        val cnt = Ignition.ignite.atomicLong("affinityRunAsync", 0, true)

        ignite$.affinityRunAsync$(CACHE_NAME, 0, () => { cnt.incrementAndGet() }, null).get
        ignite$.affinityRunAsync$(CACHE_NAME, 1, () => { cnt.incrementAndGet() }, null).get
        ignite$.affinityRunAsync$(CACHE_NAME, 2, () => { cnt.incrementAndGet() }, null).get

        assert(cnt.get === 3)
    }
}
