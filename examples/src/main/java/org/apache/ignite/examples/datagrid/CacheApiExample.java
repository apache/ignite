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

import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

/**
 * This example demonstrates some of the cache rich API capabilities.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheApiExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheApiExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException, IgniteCheckedException {

        final IgniteConfiguration conf1 = new IgniteConfiguration();
        conf1.setPeerClassLoadingEnabled(true);
        final TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        final TcpDiscoveryMulticastIpFinder finder = new TcpDiscoveryMulticastIpFinder();
        discoverySpi.setIpFinder(finder);
        conf1.setDiscoverySpi(discoverySpi);
        conf1.setGridName("grid1");

//        final IgniteConfiguration conf2 = new IgniteConfiguration(conf1);
        final IgniteConfiguration conf2 = new IgniteConfiguration();
        conf2.setPeerClassLoadingEnabled(true);
        final TcpDiscoverySpi discoverySpi2 = new TcpDiscoverySpi();
        final TcpDiscoveryMulticastIpFinder finder2 = new TcpDiscoveryMulticastIpFinder();
        discoverySpi2.setIpFinder(finder2);
        conf2.setDiscoverySpi(discoverySpi2);
        conf2.setGridName("grid2");


        try (Ignite ignite1 = Ignition.start(conf1);
             Ignite ignite2 = Ignition.start(conf2)) {
            System.out.println();
            System.out.println(">>> Cache API example started.");

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, MyPojo> cache1 = ignite1.getOrCreateCache(CACHE_NAME);
                 IgniteCache<Integer, MyPojo> cache2 = ignite2.getOrCreateCache(CACHE_NAME)) {
                // Demonstrate atomic map operations.
//                atomicMapOperations(cache);

                cache1.put(1, new MyPojo("one"));
                cache1.put(2, new MyPojo("two"));
                cache1.put(3, new MyPojo("three"));

                cache1.get(1);
                cache1.get(2);
                cache1.get(3);

                cache2.get(1);
                cache2.get(2);
                cache2.get(3);

            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite1.destroyCache(CACHE_NAME);
                ignite2.destroyCache(CACHE_NAME);
            }


        }
    }

    private static class MyPojo implements Serializable {

        private final String val;

        private MyPojo(final String val) {
            this.val = val;
        }

        private Object readResolve() {
            System.out.println("readResolve called for " + val + " " + Thread.currentThread().getName());
            final Ignite locIgnite = Ignition.localIgnite();

//            return "resolved" + locIgnite.name() + "val";
            return this;
        }
    }

    /**
     * Demonstrates cache operations similar to {@link ConcurrentMap} API. Note that
     * cache API is a lot richer than the JDK {@link ConcurrentMap}.
     *
     * @throws IgniteException If failed.
     */
    private static void atomicMapOperations(final IgniteCache<Integer, String> cache) throws IgniteException {
        System.out.println();
        System.out.println(">>> Cache atomic map operation examples.");

        // Put and return previous value.
        String v = cache.getAndPut(1, "1");
        assert v == null;

        // Put and do not return previous value.
        // Performs better when previous value is not needed.
        cache.put(2, "2");

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