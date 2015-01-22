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
 * Demonstrates a simple usage of distributed atomic long.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public final class CacheAtomicLongExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of retries */
    private static final int RETRIES = 20;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic long example started.");

            // Make name for atomic long (by which it will be known in the grid).
            String atomicName = UUID.randomUUID().toString();

            // Initialize atomic long in grid.
            final GridCacheAtomicLong atomicLong = g.cache(CACHE_NAME).dataStructures().atomicLong(atomicName, 0, true);

            System.out.println();
            System.out.println("Atomic long initial value : " + atomicLong.get() + '.');

            // Try increment atomic long from all grid nodes.
            // Note that this node is also part of the grid.
            g.compute(g.cluster().forCache(CACHE_NAME)).call(new IgniteCallable<Object>() {
                @Override public Object call() throws  Exception {
                    for (int i = 0; i < RETRIES; i++)
                        System.out.println("AtomicLong value has been incremented: " + atomicLong.incrementAndGet());

                    return null;
                }
            });

            System.out.println();
            System.out.println("Atomic long value after successful CAS: " + atomicLong.get());
        }
    }
}
