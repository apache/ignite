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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;

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
public final class CacheEntryProcessorExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheEntryProcessorExample.class.getSimpleName();

    /** Number of keys. */
    private static final int KEY_CNT = 20;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        // TODO: xml
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Entry processor example started.");

            try (IgniteCache<String, Integer> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                populateEntriesWithInvoke();

                checkEntriesInCache(cache);

                incrementEntriesWithInvoke();

                checkEntriesInCache(cache);
            }
        }
    }

    /**
     * Iterates through the cache and prints out entries.
     *
     * @param cache Cache.
     */
    private static void checkEntriesInCache(IgniteCache<String, Integer> cache) {
        System.out.println();
        System.out.println(">>> Entries in the cache.");

        for (int i = 0; i < KEY_CNT; i++)
            System.out.println("Entry: " + cache.get(String.valueOf(i)));
    }

    /**
     * Runs jobs on primary nodes with {@link IgniteCache#invoke(Object, CacheEntryProcessor, Object...)} to create
     * entries when they don't exist.
     */
    private static void populateEntriesWithInvoke() {
        Ignite ignite = Ignition.ignite();

        final IgniteCache<String, Integer> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            final String key = String.valueOf(i);

            cache.invoke(key, new EntryProcessor<String, Integer, Object>() {
                @Override
                public Object process(MutableEntry<String, Integer> entry,
                    Object... objects) throws EntryProcessorException {
                    if (entry.getValue() == null)
                        entry.setValue(new Integer(key));

                    return null;
                }
            });
        }
    }

    /**
     * Increments cache values. Jobs and entries are collocated using {@link IgniteCache#invoke(Object,
     * CacheEntryProcessor, Object...)}.
     */
    private static void incrementEntriesWithInvoke() {
        System.out.println();
        System.out.println(">>> Incrementing values.");

        Ignite ignite = Ignition.ignite();

        final IgniteCache<String, Integer> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            final String key = String.valueOf(i);

            cache.invoke(key, new EntryProcessor<String, Integer, Object>() {
                @Override
                public Object process(MutableEntry<String, Integer> entry,
                    Object... objects) throws EntryProcessorException {
                    // Peek is a local memory lookup, however, value should never be 'null'
                    // as we are co-located with node that has a given key.
                    System.out.println("Incrementing co-located entry: [key= " + key + ", value=" + cache.localPeek(key) + ']');

                    if (entry.getValue() != null)
                        entry.setValue(entry.getValue() + 1);

                    return null;
                }
            });
        }
    }
}
