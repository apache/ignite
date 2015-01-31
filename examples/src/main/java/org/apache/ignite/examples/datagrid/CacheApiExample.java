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
import org.apache.ignite.cache.*;
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
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheApiExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache API example started.");

            // Clean up caches on all nodes before run.
            g.cache(CACHE_NAME).globalClearAll(0);

            // Demonstrate atomic map operations.
            atomicMapOperations();

            // Demonstrate various ways to iterate over locally cached values.
            localIterators();
        }
    }

    /**
     * Demonstrates cache operations similar to {@link ConcurrentMap} API. Note that
     * cache API is a lot richer than the JDK {@link ConcurrentMap}.
     *
     * @throws IgniteCheckedException If failed.
     */
    private static void atomicMapOperations() throws IgniteCheckedException {
        System.out.println();
        System.out.println(">>> Cache atomic map operation examples.");

        IgniteCache<Integer, String> cache = Ignition.ignite().jcache(CACHE_NAME);

        // Put and return previous value.
        String v = cache.getAndPut(1, "1");
        assert v == null;

        // Put and do not return previous value (all methods ending with 'x' return boolean).
        // Performs better when previous value is not needed.
        cache.put(2, "2");


        // Put asynchronously (every cache operation has async counterpart).
        // TODO IGNITE-60: uncomment when implemented.
//        IgniteFuture<String> fut = cache.putAsync(3, "3");
//
//        // Asynchronously wait for result.
//        fut.listenAsync(new IgniteInClosure<IgniteFuture<String>>() {
//            @Override public void apply(IgniteFuture<String> fut) {
//                try {
//                    System.out.println("Put operation completed [previous-value=" + fut.get() + ']');
//                }
//                catch (IgniteCheckedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });

        // Put-if-absent.
        boolean b1 = cache.putIfAbsent(4, "4");
        boolean b2 = cache.putIfAbsent(4, "44");
        assert b1 && !b2;

        // Invoke - assign new value based on previous value.
        cache.put(6, "6");
        cache.invoke(6, new EntryProcessor<Integer, String, Void>() {
            @Override public Void process(MutableEntry<Integer, String> entry, Object... args) {
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

    /**
     * Demonstrates various iteration methods over locally cached values.
     */
    private static void localIterators() {
        System.out.println();
        System.out.println(">>> Local iterator examples.");

        GridCache<Integer, String> cache = Ignition.ignite().cache(CACHE_NAME);

        // Iterate over whole cache.
        for (CacheEntry<Integer, String> e : cache)
            System.out.println("Basic cache iteration [key=" + e.getKey() + ", val=" + e.getValue() + ']');

        // Iterate over cache projection for all keys below 5.
        CacheProjection<Integer, String> keysBelow5 = cache.projection(
            new IgnitePredicate<CacheEntry<Integer, String>>() {
                @Override public boolean apply(CacheEntry<Integer, String> e) {
                    return e.getKey() < 5;
                }
            }
        );

        for (CacheEntry<Integer, String> e : keysBelow5)
            System.out.println("Cache projection iteration [key=" + e.getKey() + ", val=" + e.getValue() + ']');

        // Iterate over each element using 'forEach' construct.
        cache.forEach(new IgniteInClosure<CacheEntry<Integer, String>>() {
            @Override public void apply(CacheEntry<Integer, String> e) {
                System.out.println("forEach iteration [key=" + e.getKey() + ", val=" + e.getValue() + ']');
            }
        });

        // Search cache for element with value "1" using 'forAll' construct.
        cache.forAll(new IgnitePredicate<CacheEntry<Integer, String>>() {
            @Override public boolean apply(CacheEntry<Integer, String> e) {
                String v = e.peek();

                if ("1".equals(v)) {
                    System.out.println("Found cache value '1' using forEach iteration.");

                    return false; // Stop iteration.
                }

                return true; // Continue iteration.
            }
        });
    }
}
