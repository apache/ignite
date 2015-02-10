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

import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

/**
 * Test for using grid.cache(..).projection(...) from scala code.
 */
@RunWith(classOf[JUnitRunner])
class ScalarCacheProjectionSpec extends FlatSpec {
    behavior of "Cache projection"

    it should "work properly via grid.cache(...).viewByType(...)" in scalar("examples/config/example-cache.xml") {
        val cache = ignite$.cache("local").viewByType(classOf[String], classOf[Int])

        assert(cache.putx("1", 1))
        assert(cache.get("1") == 1)
    }
}
