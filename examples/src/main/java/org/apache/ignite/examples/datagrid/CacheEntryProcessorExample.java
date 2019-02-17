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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * This example demonstrates the simplest code that populates the distributed cache and co-locates simple closure
 * execution with each key. The goal of this particular example is to provide the simplest code example of this logic
 * using EntryProcessor.
 * <p>
 * Remote nodes should always be started with special configuration file which enables P2P
 * class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node with
 * {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheEntryProcessorExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheEntryProcessorExample.class.getSimpleName();

    /** Number of keys. */
    private static final int KEY_CNT = 20;

    /** Set of predefined keys. */
    private static final Set<Integer> KEYS_SET;

    /**
     * Initializes keys set that is used in bulked operations in the example.
     */
    static {
        KEYS_SET = new HashSet<>();

        for (int i = 0; i < KEY_CNT; i++)
            KEYS_SET.add(i);
    }

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Entry processor example started.");

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // Demonstrates usage of EntryProcessor.invoke(...) method.
                populateEntriesWithInvoke(cache);

                // Demonstrates usage of EntryProcessor.invokeAll(...) method.
                incrementEntriesWithInvokeAll(cache);
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    /**
     * Populates cache with values using {@link IgniteCache#invoke(Object, EntryProcessor, Object...)} method.
     *
     * @param cache Cache that must be populated.
     */
    private static void populateEntriesWithInvoke(IgniteCache<Integer, Integer> cache) {
        // Must be no entry in the cache at this point.
        printCacheEntries(cache);

        System.out.println("");
        System.out.println(">> Populating the cache using EntryProcessor.");

        // Invokes EntryProcessor for every key sequentially.
        for (int i = 0; i < KEY_CNT; i++) {
            cache.invoke(i, (entry, object) -> {
                // Initializes entry's value if it's not set.
                if (entry.getValue() == null)
                    entry.setValue((entry.getKey() + 1) * 10);
                return null;
            });
        }

        // Print outs entries that are set using the EntryProcessor above.
        printCacheEntries(cache);
    }

    /**
     * Increments values of entries stored in the cache using {@link IgniteCache#invokeAll(Set, EntryProcessor,
     * Object...)} method.
     *
     * @param cache Cache instance.
     */
    private static void incrementEntriesWithInvokeAll(IgniteCache<Integer, Integer> cache) {
        System.out.println("");
        System.out.println(">> Incrementing values in the cache using EntryProcessor.");

        // Using EntryProcessor.invokeAll to increment every value in place.
        cache.invokeAll(KEYS_SET, (entry, object) -> {
            entry.setValue(entry.getValue() + 5);

            return null;
        });

        // Print outs entries that are incremented using the EntryProcessor above.
        printCacheEntries(cache);
    }

    /**
     * Prints out all the entries that are stored in a cache.
     *
     * @param cache Cache.
     */
    private static void printCacheEntries(IgniteCache<Integer, Integer> cache) {
        System.out.println();
        System.out.println(">>> Entries in the cache.");

        Map<Integer, Integer> entries = cache.getAll(KEYS_SET);

        if (entries.isEmpty())
            System.out.println("No entries in the cache.");
        else {
            for (Map.Entry<Integer, Integer> entry : entries.entrySet())
                System.out.println("Entry [key=" + entry.getKey() + ", value=" + entry.getValue() + ']');
        }
    }
}
