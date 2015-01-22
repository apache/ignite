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

package org.apache.ignite.examples.datagrid.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.examples.datagrid.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Demonstrates a simple usage of distributed count down latch.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheCountDownLatchExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of latch initial count */
    private static final int INITIAL_COUNT = 10;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic countdown latch example started.");

            // Make name of count down latch.
            final String latchName = UUID.randomUUID().toString();

            // Initialize count down latch in grid.
            GridCacheCountDownLatch latch = g.cache(CACHE_NAME).dataStructures().
                countDownLatch(latchName, INITIAL_COUNT, false, true);

            System.out.println("Latch initial value: " + latch.count());

            // Start waiting on the latch on all grid nodes.
            for (int i = 0; i < INITIAL_COUNT; i++)
                g.compute().run(new LatchClosure(CACHE_NAME, latchName));

            // Wait for latch to go down which essentially means that all remote closures completed.
            latch.await();

            System.out.println("All latch closures have completed.");
        }

        System.out.println();
        System.out.println("Finished count down latch example...");
        System.out.println("Check all nodes for output (this node is also part of the grid).");
    }

    /**
     * Closure which simply waits on the latch on all nodes.
     */
    private static class LatchClosure implements IgniteRunnable {
        /** Cache name. */
        private final String cacheName;

        /** Latch name. */
        private final String latchName;

        /**
         * @param cacheName Cache name.
         * @param latchName Latch name.
         */
        LatchClosure(String cacheName, String latchName) {
            this.cacheName = cacheName;
            this.latchName = latchName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridCacheCountDownLatch latch = Ignition.ignite().cache(cacheName).dataStructures().
                    countDownLatch(latchName, 1, false, true);

                int newCnt = latch.countDown();

                System.out.println("Counted down [newCnt=" + newCnt + ", nodeId=" + Ignition.ignite().cluster().localNode().id() + ']');
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
