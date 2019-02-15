/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.datagrid;

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;

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
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache API example started.");

            CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setName(CACHE_NAME);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cfg)) {
                // Demonstrate atomic map operations.
                atomicMapOperations(cache);
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
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

        // Put and do not return previous value (all methods ending with 'x' return boolean).
        // Performs better when previous value is not needed.
        cache.put(2, "2");

        // Put-if-absent.
        boolean b1 = cache.putIfAbsent(4, "4");
        boolean b2 = cache.putIfAbsent(4, "44");
        assert b1 && !b2;

        // Invoke - assign new value based on previous value.
        cache.put(6, "6");

        cache.invoke(6, (entry, args) -> {
            String val = entry.getValue();

            entry.setValue(val + "6"); // Set new value based on previous value.

            return null;
        });

        // Replace.
        cache.put(7, "7");
        b1 = cache.replace(7, "7", "77");
        b2 = cache.replace(7, "7", "777");
        assert b1 & !b2;
    }
}
