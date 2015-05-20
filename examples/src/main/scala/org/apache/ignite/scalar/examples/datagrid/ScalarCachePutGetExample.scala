/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{IgniteCache, IgniteException}

import java.util.{HashMap => JavaMap}

import scala.collection.JavaConversions._

/**
 * This example demonstrates very basic operations on cache, such as 'put' and 'get'.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCachePutGetExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val CACHE_NAME = ScalarCachePutGetExample.getClass.getSimpleName

    scalar(CONFIG) {
        val cache = createCache$[Integer, String](CACHE_NAME)

        try {
            putGet(cache)
            putAllGetAll(cache)
        }
        finally {
            if (cache != null) cache.close()
        }
    }
    
    /**
     * Execute individual puts and gets.
     *
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def putGet(cache: IgniteCache[Integer, String]) {
        println()
        println(">>> Cache put-get example started.")

        val keyCnt = 20

        for (i <- 0 until keyCnt)
            cache.put(i, Integer.toString(i))

        println(">>> Stored values in cache.")

        for (i <- 0 until keyCnt)
            println("Got [key=" + i + ", val=" + cache.get(i) + ']')
    }

    /**
     * Execute bulk `putAll(...)` and `getAll(...)` operations.
     *
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def putAllGetAll(cache: IgniteCache[Integer, String]) {
        println()
        println(">>> Starting putAll-getAll example.")

        val keyCnt = 20

        val batch = new JavaMap[Integer, String]

        for (i <- 0 until keyCnt)
            batch.put(i, "bulk-" + Integer.toString(i))

        cache.putAll(batch)

        println(">>> Bulk-stored values in cache.")

        val vals = cache.getAll(batch.keySet)

        for (e <- vals.entrySet)
            println("Got entry [key=" + e.getKey + ", val=" + e.getValue + ']')
    }
}
