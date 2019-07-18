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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.PART_ATOMIC_CACHE;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertCachesReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertDataStreamerReadOnlyMode;

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
    public void testDataStreamerReadOnlyConcurrent()throws Exception {
        final int totalEntries = 50_000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicBoolean exThrown = new AtomicBoolean();
        final CountDownLatch finishLatch = new CountDownLatch(1);

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (IgniteDataStreamer<Integer, Integer> streamer = grid(0).dataStreamer(PART_ATOMIC_CACHE)) {
                    startLatch.countDown();
                    streamer.allowOverwrite(false);

                    for (int i = 0; i < totalEntries; i++) {
                        if ((i + 1) % (totalEntries / 10) == 0)
                            log.info("Prepared " + (i + 1) * 100 / (totalEntries) + "% entries.");

                        streamer.addData(i, i + PART_ATOMIC_CACHE.hashCode());
                    }
                }
                catch (CacheException e) {
                    exThrown.compareAndSet(false, true);

                    log.info("Streamer cache exception is thrown", e.getMessage());
                }
                finally {
                    finishLatch.countDown();
                }
            }
        });

        startLatch.await(5, TimeUnit.SECONDS);

        changeClusterReadOnlyMode(true);

        assertCachesReadOnlyMode(true, CACHE_NAMES);

        changeClusterReadOnlyMode(false);

        finishLatch.await(5, TimeUnit.SECONDS);

        assertCachesReadOnlyMode(false, CACHE_NAMES);

        assertTrue("DataStreamer exception not thrown during change readonly mode", exThrown.get());
    }
}
