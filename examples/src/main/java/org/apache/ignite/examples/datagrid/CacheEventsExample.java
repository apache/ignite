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
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;

import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * This examples demonstrates events API. Note that grid events are disabled by default and
 * must be specifically enabled, just like in {@code examples/config/example-cache.xml} file.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheEventsExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException, InterruptedException {
        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache events example started.");

            final Cache<Integer, String> cache = g.cache(CACHE_NAME);

            // Clean up caches on all nodes before run.
            cache.globalClearAll(0);

            // This optional local callback is called for each event notification
            // that passed remote predicate listener.
            IgniteBiPredicate<UUID, IgniteCacheEvent> locLsnr = new IgniteBiPredicate<UUID, IgniteCacheEvent>() {
                @Override public boolean apply(UUID uuid, IgniteCacheEvent evt) {
                    System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() +
                        ", oldVal=" + evt.oldValue() + ", newVal=" + evt.newValue());

                    return true; // Continue listening.
                }
            };

            // Remote listener which only accepts events for keys that are
            // greater or equal than 10 and if event node is primary for this key.
            IgnitePredicate<IgniteCacheEvent> rmtLsnr = new IgnitePredicate<IgniteCacheEvent>() {
                @Override public boolean apply(IgniteCacheEvent evt) {
                    System.out.println("Cache event [name=" + evt.name() + ", key=" + evt.key() + ']');

                    int key = evt.key();

                    return key >= 10 && cache.affinity().isPrimary(g.cluster().localNode(), key);
                }
            };

            // Subscribe to specified cache events on all nodes that have cache running.
            // Cache events are explicitly enabled in examples/config/example-cache.xml file.
            g.events(g.cluster().forCache(CACHE_NAME)).remoteListen(locLsnr, rmtLsnr,
                EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED);

            // Generate cache events.
            for (int i = 0; i < 20; i++)
                cache.putx(i, Integer.toString(i));

            // Wait for a while while callback is notified about remaining puts.
            Thread.sleep(2000);
        }
    }
}
