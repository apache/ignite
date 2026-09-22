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

package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Negative test for the size-aware eviction progress guard. */
public class PageEvictionGuardOomTest extends PageEvictionAbstractTest {
    /** Off-heap region size. */
    private static final int SIZE = 12 * 1024 * 1024;

    /**
     * Number of resident entries (each ~one page) filling the region to ~55% of its capacity. This keeps the region
     * comfortably below the eviction threshold (so the ordinary threshold-based {@code ensureFreeSpace} path is a
     * no-op) while leaving less free space than a single large record needs, so the size-aware eviction guard is
     * exercised.
     */
    private static final int FILL_ENTRIES = 1_600;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(SIZE)
                    .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Filling the region with locked entries and then writing a row that needs more free pages than remain must fail
     * with OOM (bounded time), not hang: eviction cannot free any page because every candidate entry is locked.
     *
     * @throws Exception If failed.
     */
    @Test(timeout = 180_000)
    public void testGuardOomWhenAllEntriesLocked() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME, 0, true);

        // Pre-fill the region so that less than one large record of free space remains, without overflowing it.
        byte[] fillVal = new byte[3_800];

        for (int i = 1; i <= FILL_ENTRIES; i++)
            cache.put(i, fillVal);

        Collection<Integer> keys = new ArrayList<>(FILL_ENTRIES);

        for (int i = 1; i <= FILL_ENTRIES; i++)
            keys.add(i);

        CountDownLatch ready = new CountDownLatch(1);

        CountDownLatch release = new CountDownLatch(1);

        // Hold entry locks on every resident key from a background thread so that eviction has no evictable page.
        IgniteInternalFuture<?> lockerFut = GridTestUtils.runAsync(() -> {
            Lock lock = cache.lockAll(keys);

            lock.lock();

            try {
                ready.countDown();

                release.await();
            }
            finally {
                lock.unlock();
            }
        }, "size-aware-guard-locker");

        try {
            assertTrue("Timed out waiting for entries to be locked", ready.await(60, TimeUnit.SECONDS));

            GridTestUtils.assertThrowsWithCause(
                () -> cache.put(FILL_ENTRIES + 1, new byte[8 * 1024 * 1024]),
                IgniteOutOfMemoryException.class
            );
        }
        finally {
            release.countDown();

            lockerFut.get(10, TimeUnit.SECONDS);
        }
    }
}
