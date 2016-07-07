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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.examples.ExampleNodeStartup;


/**
 * This example demonstrates very basic operations on cache, such as size.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheClientCacheSizeExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheClientCacheSizeExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-client.xml")) {
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // Individual puts and gets.
                putGet(cache);
                System.out.println(">>> cache size is " +cache.size());
                System.out.println(">>> cache partition are "+ignite.affinity(CACHE_NAME).partitions());
                System.out.println(">>> cache size in primary partition "+ 0 +"is "+cache.size(0, CachePeekMode.PRIMARY));
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

        final int keyCnt = 5000;

        // Store keys in cache.
        for (int i = 0; i < keyCnt; i++)
            cache.put(i, Integer.toString(i));

        System.out.println(">>> Stored values in cache.");

        for (int i = 0; i < keyCnt; i++)
            System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
    }
}
