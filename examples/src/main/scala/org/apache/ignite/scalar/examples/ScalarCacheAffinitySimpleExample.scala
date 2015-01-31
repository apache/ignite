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

package org.apache.ignite.scalar.examples

import org.apache.ignite.cache.GridCache
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * This example demonstrates the simplest code that populates the distributed cache
 * and co-locates simple closure execution with each key. The goal of this particular
 * example is to provide the simplest code example of this logic.
 *
 * Note that other examples in this package provide more detailed examples
 * of affinity co-location.
 *
 * Note also that for affinity routing is enabled for all caches.
 *
 * Remote nodes should always be started with configuration file which includes
 * cache: `'ignite.sh examples/config/example-cache.xml'`. Local node can
 * be started with or without cache.
 */
object ScalarCacheAffinitySimpleExample extends App {
    /** Number of keys. */
    private val KEY_CNT = 20

    /** Name of cache specified in spring configuration. */
    private val NAME = "partitioned"

    /** Type alias. */
    type Cache = GridCache[Int, String]

    /*
     * Note that in case of `LOCAL` configuration,
     * since there is no distribution, values may come back as `nulls`.
     */
    scalar("examples/config/example-cache.xml") {
        // Clean up caches on all nodes before run.
        cache$(NAME).get.globalClearAll(0)

        val c = grid$.cache[Int, String](NAME)

        populate(c)
        visit(c)
    }

    /**
     * Visits every in-memory data grid entry on the remote node it resides by co-locating visiting
     * closure with the cache key.
     *
     * @param c Cache to use.
     */
    private def visit(c: Cache) {
        (0 until KEY_CNT).foreach(i =>
            grid$.compute().affinityRun(NAME, i,
                () => println("Co-located [key= " + i + ", value=" + c.peek(i) + ']'))
        )
    }

    /**
     * Populates given cache.
     *
     * @param c Cache to populate.
     */
    private def populate(c: Cache) {
        (0 until KEY_CNT).foreach(i => c += (i -> i.toString))
    }
}
