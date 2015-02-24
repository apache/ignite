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
import org.apache.ignite.lang.*;

import javax.cache.processor.*;
import java.util.concurrent.*;

/**
 * This example demonstrates some of the cache rich API capabilities.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheApiExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache API example started.");

            // Clean up caches on all nodes before run.
            ignite.jcache(CACHE_NAME).clear();

            // Demonstrate atomic map operations.
            atomicMapOperations();
        }
    }

    /**
     * Demonstrates cache operations similar to {@link ConcurrentMap} API. Note that
     * cache API is a lot richer than the JDK {@link ConcurrentMap}.
     *
     * @throws IgniteException If failed.
     */
    private static void atomicMapOperations() throws IgniteException {
        System.out.println();
        System.out.println(">>> Cache atomic map operation examples.");

        final IgniteCache<Integer, String> cache = Ignition.ignite().jcache(CACHE_NAME);

        // Put and return previous value.
        String v = cache.getAndPut(1, "1");
        assert v == null;

        // Put and do not return previous value (all methods ending with 'x' return boolean).
        // Performs better when previous value is not needed.
        cache.put(2, "2");


        // Put asynchronously.
        final IgniteCache<Integer, String> asyncCache = cache.withAsync();

        asyncCache.put(3, "3");

        asyncCache.get(3);

        IgniteFuture<String> fut = asyncCache.future();

        //Asynchronously wait for result.
        fut.listenAsync(new IgniteInClosure<IgniteFuture<String>>() {
            @Override
            public void apply(IgniteFuture<String> fut) {
                try {
                    System.out.println("Put operation completed [previous-value=" + fut.get() + ']');
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }
            }
        });

        // Put-if-absent.
        boolean b1 = cache.putIfAbsent(4, "4");
        boolean b2 = cache.putIfAbsent(4, "44");
        assert b1 && !b2;

        // Invoke - assign new value based on previous value.
        cache.put(6, "6");
        cache.invoke(6, new EntryProcessor<Integer, String, Object>() {
            @Override public Object process(MutableEntry<Integer, String> entry, Object... args) {
                String v = entry.getValue();

                entry.setValue(v + "6"); // Set new value based on previous value.

                return null;
            }
        });

        // Replace.
        cache.put(7, "7");
        b1 = cache.replace(7, "7", "77");
        b2 = cache.replace(7, "7", "777");
        assert b1 & !b2;
    }
}
