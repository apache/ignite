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

import org.apache.ignite.IgniteDataStreamer
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates how cache can be populated with data utilizing [[IgniteDataStreamer]] API.
 * [[IgniteDataStreamer]] is a lot more efficient to use than standard
 * `put(...)` operation as it properly buffers cache requests
 * together and properly manages load on remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheDataStreamerExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"
    
    /** Cache name. */
    private val CACHE_NAME = ScalarCacheDataStreamerExample.getClass.getSimpleName

    /** Number of entries to load. */
    private val ENTRY_COUNT = 500000

    /** Heap size required to run this example. */
    val MIN_MEMORY = 512 * 1024 * 1024

    ExamplesUtils.checkMinMemory(MIN_MEMORY)

    scalar(CONFIG) {
        println()
        println(">>> Cache data streamer example started.")

        val cache = createCache$(CACHE_NAME)

        try {
            val start = System.currentTimeMillis

            val stmr = dataStreamer$[Integer, String](CACHE_NAME)

            try {
                stmr.perNodeBufferSize(1024)
                stmr.perNodeParallelOperations(8)

                for (i <- 0 until ENTRY_COUNT) {
                    stmr.addData(i, Integer.toString(i))

                    if (i > 0 && i % 10000 == 0)
                        println("Loaded " + i + " keys.")
                }
            }
            finally {
                stmr.close()
            }

            val end = System.currentTimeMillis

            println(">>> Loaded " + ENTRY_COUNT + " keys in " + (end - start) + "ms.")
        }
        finally {
            cache.close()
        }
    }
}
