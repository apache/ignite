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

package org.apache.ignite.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;

/**
 * Demonstrates how cache can be populated with data utilizing {@link IgniteDataStreamer} API.
 * {@link IgniteDataStreamer} is a lot more efficient to use than standard
 * {@code put(...)} operation as it properly buffers cache requests
 * together and properly manages load on remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheDataStreamerExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 500000;

    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 512 * 1024 * 1024;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache data streamer example started.");

            // Clean up caches on all nodes before run.
            ignite.jcache(CACHE_NAME).clear();

            System.out.println();
            System.out.println(">>> Cache clear finished.");

            long start = System.currentTimeMillis();

            try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer(CACHE_NAME)) {
                // Configure loader.
                stmr.perNodeBufferSize(1024);
                stmr.perNodeParallelOperations(8);

                for (int i = 0; i < ENTRY_COUNT; i++) {
                    stmr.addData(i, Integer.toString(i));

                    // Print out progress while loading cache.
                    if (i > 0 && i % 10000 == 0)
                        System.out.println("Loaded " + i + " keys.");
                }
            }

            long end = System.currentTimeMillis();

            System.out.println(">>> Loaded " + ENTRY_COUNT + " keys in " + (end - start) + "ms.");
        }
    }
}
