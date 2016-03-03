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

import javax.cache.processor.{EntryProcessor, MutableEntry}

import org.apache.ignite.IgniteCache
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * This example demonstrates the simplest code that populates the distributed cache
 * and co-locates simple closure execution with each key. The goal of this particular
 * example is to provide the simplest code example of this logic using EntryProcessor.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
object ScalarCacheEntryProcessorExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Name of cache. */
    private val CACHE_NAME = ScalarCacheEntryProcessorExample.getClass.getSimpleName

    /** Number of keys. */
    private val KEY_CNT = 20

    /** Type alias. */
    type Cache = IgniteCache[String, Int]

    /*
     * Note that in case of `LOCAL` configuration,
     * since there is no distribution, values may come back as `nulls`.
     */
    scalar(CONFIG) {
        println()
        println(">>> Entry processor example started.")

        val cache = createCache$[String, Int](CACHE_NAME)

        try {
            populateEntriesWithInvoke(cache)

            checkEntriesInCache(cache)

            incrementEntriesWithInvoke(cache)

            checkEntriesInCache(cache)
        }
        finally {
            cache.destroy()
        }
    }

    private def checkEntriesInCache(cache: Cache) {
        println()
        println(">>> Entries in the cache.")

        (0 until KEY_CNT).foreach(i =>
            println("Entry: " + cache.get(i.toString)))
    }

    /**
     * Runs jobs on primary nodes with {@link IgniteCache#invoke(Object, CacheEntryProcessor, Object...)} to create
     * entries when they don't exist.
     *
     * @param cache Cache to populate.
     */
    private def populateEntriesWithInvoke(cache: Cache) {
        (0 until KEY_CNT).foreach(i =>
            cache.invoke(i.toString,
                new EntryProcessor[String, Int, Object]() {
                    override def process(e: MutableEntry[String, Int], args: AnyRef*): Object = {
                        if (e.getValue == null)
                            e.setValue(i)

                        null
                    }
                }
            )
        )
    }

    /**
     * Runs jobs on primary nodes with {@link IgniteCache#invoke(Object, CacheEntryProcessor, Object...)} to increment
     * entries values.
     *
     * @param cache Cache to populate.
     */
    private def incrementEntriesWithInvoke(cache: Cache) {
        println()
        println(">>> Incrementing values.")

        (0 until KEY_CNT).foreach(i =>
            cache.invoke(i.toString,
                new EntryProcessor[String, Int, Object]() {
                    override def process(e: MutableEntry[String, Int], args: AnyRef*): Object = {
                        Option(e.getValue) foreach (v => e.setValue(v + 1))

                        null
                    }
                }
            )
        )
    }
}
