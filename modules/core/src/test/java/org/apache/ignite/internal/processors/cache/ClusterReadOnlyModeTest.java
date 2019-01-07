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

package org.apache.ignite.internal.processors.cache;

import java.util.Random;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.typedef.G;

/**
 * Tests cache get/put/remove and data streaming in read-only cluster mode.
 */
public class ClusterReadOnlyModeTest extends ClusterReadOnlyModeAbstractTest {
    /**
     * Tests cache get/put/remove.
     */
    public void testCacheGetPutRemove() {
        assertCachesReadOnlyMode(false);

        changeClusterReadOnlyMode(true);

        assertCachesReadOnlyMode(true);

        changeClusterReadOnlyMode(false);

        assertCachesReadOnlyMode(false);
    }

    /**
     * Tests data streamer.
     */
    public void testDataStreamerReadOnly() {
        assertDataStreamerReadOnlyMode(false);

        changeClusterReadOnlyMode(true);

        assertDataStreamerReadOnlyMode(true);

        changeClusterReadOnlyMode(false);

        assertDataStreamerReadOnlyMode(false);
    }

    /**
     * Asserts that all caches in read-only or in read/write mode on all nodes.
     *
     * @param readOnly If {@code true} then cache must be in read only mode, else in read/write mode.
     */
    private void assertCachesReadOnlyMode(boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                for (int i = 0; i < 10; i++) {
                    cache.get(rnd.nextInt(100)); // All gets must succeed.

                    if (readOnly) {
                        // All puts must fail.
                        try {
                            cache.put(rnd.nextInt(100), rnd.nextInt());

                            fail("Put must fail for cache " + cacheName);
                        }
                        catch (Exception e) {
                            // No-op.
                        }

                        // All removes must fail.
                        try {
                            cache.remove(rnd.nextInt(100));

                            fail("Remove must fail for cache " + cacheName);
                        }
                        catch (Exception e) {
                            // No-op.
                        }
                    }
                    else {
                        cache.put(rnd.nextInt(100), rnd.nextInt()); // All puts must succeed.

                        cache.remove(rnd.nextInt(100)); // All removes must succeed.
                    }
                }
            }
        }
    }

    /**
     * @param readOnly If {@code true} then data streamer must fail, else succeed.
     */
    private void assertDataStreamerReadOnlyMode(boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                boolean failed = false;

                try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
                    for (int i = 0; i < 10; i++) {
                        ((DataStreamerImpl)streamer).maxRemapCount(5);

                        streamer.addData(rnd.nextInt(1000), rnd.nextInt());
                    }
                }
                catch (CacheException ignored) {
                    failed = true;
                }

                if (failed != readOnly)
                    fail("Streaming to " + cacheName + " must " + (readOnly ? "fail" : "succeed"));
            }
        }
    }
}
