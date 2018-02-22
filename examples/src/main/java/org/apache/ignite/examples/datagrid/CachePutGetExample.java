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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * This example demonstrates very basic operations on cache, such as 'put' and 'get'.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CachePutGetExample {
    /** Cache name. */
    private static final String CACHE_NAME = CachePutGetExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // Individual puts and gets.
                putGet(cache);

                // Bulk puts and gets.
                putAllGetAll(cache);
            }
        }
    }

    /**
     * Execute individual puts and gets.
     *
     * @throws IgniteException If failed.
     */
    private static void putGet(IgniteCache<Integer, String> cache) throws IgniteException {
        System.out.println();
        System.out.println(">>> Cache put-get example started.");

        final int keyCnt = 20;

        // Store keys in cache.
        for (int i = 0; i < keyCnt; i++)
            cache.put(i, Integer.toString(i));

        System.out.println(">>> Stored values in cache.");

        for (int i = 0; i < keyCnt; i++)
            System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
    }

    /**
     * Execute bulk {@code putAll(...)} and {@code getAll(...)} operations.
     *
     * @throws IgniteException If failed.
     */
    private static void putAllGetAll(IgniteCache<Integer, String> cache) throws IgniteException {
        System.out.println();
        System.out.println(">>> Starting putAll-getAll example.");

        final int keyCnt = 20;

        // Create batch.
        Map<Integer, String> batch = new HashMap<>();

        for (int i = 0; i < keyCnt; i++)
            batch.put(i, "bulk-" + Integer.toString(i));

        // Bulk-store entries in cache.
        cache.putAll(batch);

        System.out.println(">>> Bulk-stored values in cache.");

        // Bulk-get values from cache.
        Map<Integer, String> vals = cache.getAll(batch.keySet());

        for (Map.Entry<Integer, String> e : vals.entrySet())
            System.out.println("Got entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
    }
}
