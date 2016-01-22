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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * This examples demonstrates events API. Note that ignite events are disabled by default and
 * must be specifically enabled, just like in {@code examples/config/example-ignite.xml} file.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheEventsExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheEventsExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException, InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache events example started.");

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // This optional local callback is called for each event notification
                // that passed remote predicate listener.
                IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                    @Override public boolean apply(UUID uuid, CacheEvent evt) {
                        System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() +
                            ", oldVal=" + evt.oldValue() + ", newVal=" + evt.newValue());

                        return true; // Continue listening.
                    }
                };

                // Remote listener which only accepts events for keys that are
                // greater or equal than 10 and if event node is primary for this key.
                IgnitePredicate<CacheEvent> rmtLsnr = new IgnitePredicate<CacheEvent>() {
                    @Override public boolean apply(CacheEvent evt) {
                        System.out.println("Cache event [name=" + evt.name() + ", key=" + evt.key() + ']');

                        int key = evt.key();

                        return key >= 10 && ignite.affinity(CACHE_NAME).isPrimary(ignite.cluster().localNode(), key);
                    }
                };

                // Subscribe to specified cache events on all nodes that have cache running.
                // Cache events are explicitly enabled in examples/config/example-ignite.xml file.
                ignite.events(ignite.cluster().forCacheNodes(CACHE_NAME)).remoteListen(locLsnr, rmtLsnr,
                    EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);

                // Generate cache events.
                for (int i = 0; i < 20; i++)
                    cache.put(i, Integer.toString(i));

                // Wait for a while while callback is notified about remaining puts.
                Thread.sleep(2000);
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }
}