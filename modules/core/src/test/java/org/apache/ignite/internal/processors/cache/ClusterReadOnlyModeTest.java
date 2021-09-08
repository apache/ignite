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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertCachesReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertDataStreamerReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;

/**
 * Tests cache get/put/remove and data streaming in read-only cluster mode.
 */
public class ClusterReadOnlyModeTest extends ClusterReadOnlyModeAbstractTest {
    /**
     * Tests cache get/put/remove.
     */
    @Test
    public void testCacheGetPutRemove() {
        assertCachesReadOnlyMode(false, CACHE_NAMES);

        changeClusterReadOnlyMode(true);

        assertCachesReadOnlyMode(true, CACHE_NAMES);

        changeClusterReadOnlyMode(false);

        assertCachesReadOnlyMode(false, CACHE_NAMES);
    }

    /**
     * Tests data streamer.
     */
    @Test
    public void testDataStreamerReadOnly() {
        assertDataStreamerReadOnlyMode(false, CACHE_NAMES);

        changeClusterReadOnlyMode(true);

        assertDataStreamerReadOnlyMode(true, CACHE_NAMES);

        changeClusterReadOnlyMode(false);

        assertDataStreamerReadOnlyMode(false, CACHE_NAMES);
    }

    /**
     * Tests data streamer.
     */
    @Test
    public void testDataStreamerReadOnlyConcurrent() throws Exception {
        testDataStreamerReadOnlyConcurrent(false, false);
    }

    /**
     * Tests data streamer.
     */
    @Test
    public void testDataStreamerReadOnlyConcurrentWithFlush() throws Exception {
        testDataStreamerReadOnlyConcurrent(true, false);
    }

    /**
     * Tests data streamer.
     */
    @Test
    public void testDataStreamerReadOnlyConcurrentAllowOverride() throws Exception {
        testDataStreamerReadOnlyConcurrent(false, true);
    }

    /**
     * Tests data streamer.
     */
    @Test
    public void testDataStreamerReadOnlyConcurrentWithFlushAllowOverride() throws Exception {
        testDataStreamerReadOnlyConcurrent(true, true);
    }

    /**
     * Common logic for different datastreamers' tests.
     *
     * @param manualFlush If {@code True} {@link IgniteDataStreamer#flush()} will be invoked in the each batch load.
     * @param allowOverride value for {@link IgniteDataStreamer#allowOverwrite(boolean)} method.
     * @throws Exception If something goes wrong.
     */
    private void testDataStreamerReadOnlyConcurrent(boolean manualFlush, boolean allowOverride) throws Exception {
        final CountDownLatch firstPackLatch = new CountDownLatch(cacheNames().size());
        final CountDownLatch finishLatch = new CountDownLatch(cacheNames().size());

        final CountDownLatch readOnlyEnabled = new CountDownLatch(1);

        final Map<String, Exception> eMap = new ConcurrentHashMap<>(cacheNames().size());

        Map<String, IgniteInternalFuture<?>> futs = new HashMap<>(cacheNames().size());

        try {
            for (String cacheName : cacheNames()) {
                futs.put(cacheName, GridTestUtils.runAsync(() -> {
                    try (IgniteDataStreamer<Integer, Integer> streamer = grid(0).dataStreamer(cacheName)) {
                        streamer.allowOverwrite(allowOverride);

                        doLoad(streamer, 0, 100, manualFlush);

                        firstPackLatch.countDown();

                        assertTrue(cacheName, readOnlyEnabled.await(60, TimeUnit.SECONDS));

                        doLoad(streamer, 100, 1000000, manualFlush);

                        finishLatch.countDown();
                    }
                    catch (Exception e) {
                        log.error("Streamer cache exception is thrown for cache " + cacheName, e);

                        assertNull(cacheName, eMap.put(cacheName, e));
                    }
                    finally {
                        // Avoid to hanging test in case of unexpected behaviour.
                        firstPackLatch.countDown();
                        finishLatch.countDown();
                    }
                }));
            }

            assertTrue(firstPackLatch.await(60, TimeUnit.SECONDS));

            changeClusterReadOnlyMode(true);

            readOnlyEnabled.countDown();

            assertTrue(finishLatch.await(60, TimeUnit.SECONDS));

            assertEquals("exceptions: " + eMap, cacheNames().size(), eMap.size());

            for (String cacheName : cacheNames()) {
                Exception e = eMap.get(cacheName);

                assertNotNull(cacheName, e);
                assertTrue(cacheName + " " + e, X.hasCause(e, IgniteClusterReadOnlyException.class));
            }
        }
        finally {
            // Avoid to hanging test in case of unexpected behaviour.
            readOnlyEnabled.countDown();

            awaitThreads(futs);
        }
    }

    /**
     *
     */
    private void awaitThreads(Map<String, IgniteInternalFuture<?>> futs) {
        for (String cacheName : futs.keySet()) {
            IgniteInternalFuture<?> fut = futs.get(cacheName);

            try {
                fut.get(15, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                log.error("Failed to get future " + cacheName, e);

                try {
                    fut.cancel();
                }
                catch (IgniteCheckedException ce) {
                    log.error("Failed to cancel future " + cacheName, e);
                }
            }
        }
    }

    /**
     *
     */
    private void doLoad(IgniteDataStreamer<Integer, Integer> streamer, int from, int count, boolean flush) {
        assertTrue(count > 0);

        for (int i = from; i < from + count; i++)
            streamer.addData(i, i);

        if (flush)
            streamer.flush();
    }
}
