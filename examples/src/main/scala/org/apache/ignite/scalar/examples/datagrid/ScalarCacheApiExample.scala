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

package org.apache.ignite.scalar.examples.datagrid

import java.util.concurrent.ConcurrentMap
import javax.cache.processor.{EntryProcessor, MutableEntry}

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{IgniteCache, IgniteException}

/**
 * This example demonstrates some of the cache rich API capabilities.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheApiExample extends App{
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val CACHE_NAME = ScalarCacheApiExample.getClass.getSimpleName

    scalar(CONFIG) {
        println()
        println(">>> Cache API example started.")

        val cache = createCache$[Integer, String](CACHE_NAME)

        try {
            atomicMapOperations(cache)
        }
        finally {
            if (cache != null) cache.close()
        }
    }

    /**
     * Demonstrates cache operations similar to [[ConcurrentMap]] API. Note that
     * cache API is a lot richer than the JDK [[ConcurrentMap]].
     *
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def atomicMapOperations(cache: IgniteCache[Integer, String]) {
        println()
        println(">>> Cache atomic map operation examples.")

        val v = cache.getAndPut(1, "1")

        assert(v == null)

        cache.put(2, "2")

        var b1 = cache.putIfAbsent(4, "4")
        var b2 = cache.putIfAbsent(4, "44")

        assert(b1 && !b2)

        cache.put(6, "6")
        cache.invoke(6, new EntryProcessor[Integer, String, AnyRef] {
            @impl def process(entry: MutableEntry[Integer, String], args: AnyRef*): AnyRef = {
                val v = entry.getValue

                entry.setValue(v + "6")

                null
            }
        })

        cache.put(7, "7")

        b1 = cache.replace(7, "7", "77")
        b2 = cache.replace(7, "7", "777")

        assert(b1 & !b2)
    }
}
