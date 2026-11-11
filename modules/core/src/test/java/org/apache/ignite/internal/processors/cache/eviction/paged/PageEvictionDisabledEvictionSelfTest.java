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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Checks that the size-aware page reservation does not change the behavior of an in-memory data region with disabled
 * page eviction: inserts (including read-through loads) are still rejected by the out-of-memory check at the threshold,
 * while removals still succeed and do not trigger the failure handler.
 */
public class PageEvictionDisabledEvictionSelfTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.DISABLED, super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testRemoveAtOomThreshold() throws Exception {
        IgniteEx ignite = startGrids(2);

        IgniteCache<Integer, byte[]> cache = ignite.createCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        awaitPartitionMapExchange();

        int filled = fillUntilOom(ignite, cache);

        OOM_FAILURES.set(0);

        int half = filled / 2;

        // Primary single-key removes (and backup deletes on the other node).
        for (int i = 0; i < half; i++)
            cache.remove(i);

        // Primary batch removes. The key that failed with OOM is included: it may be stored on one of the nodes.
        Set<Integer> keys = new HashSet<>();

        for (int i = half; i <= filled; i++)
            keys.add(i);

        cache.removeAll(keys);

        assertEquals(0, cache.size());
        assertEquals("Removals must not report out-of-memory to the failure handler", 0, OOM_FAILURES.get());
    }

    /**
     * The size-aware reservation is not engaged for a region with disabled page eviction: it neither reserves nor
     * performs an early out-of-memory check, even for a row larger than the region. The out-of-memory check of an
     * actual insert ({@code ensureFreeSpaceForInsert}) is kept as is.
     */
    @Test
    public void testReservationNotEngaged() throws Exception {
        IgniteEx ignite = startGrid(0);

        createCache(ignite, DEFAULT_CACHE_NAME).put(0, 0);

        DataRegion region = defaultRegion(ignite);

        FreeList<?> freeList = ignite.context().cache().context().database().freeList(region.config().getName());

        int hugeRow = (int)Math.min(Integer.MAX_VALUE, regionMaxSize(ignite) * 2);

        assertEquals(0, ignite.context().cache().context().database().reserveInRegion(region, hugeRow, false));
        assertEquals(0, freeList.reserve(hugeRow, false));
        assertEquals(0, OOM_FAILURES.get());

        GridTestUtils.assertThrows(log, () -> {
            ignite.context().cache().context().database().ensureFreeSpaceForInsert(region, hugeRow);

            return null;
        }, IgniteOutOfMemoryException.class, null);
    }

    /** Read-through load into a region at the out-of-memory threshold is rejected by the insert check. */
    @Test
    public void testReadThroughGetOomAtThreshold() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(readThroughCacheConfig());

        int filled = fillUntilOom(ignite, cache);

        GridTestUtils.assertThrowsWithCause(() -> cache.get(filled + 1), IgniteOutOfMemoryException.class);
    }

    /** Read-through load below the threshold succeeds. */
    @Test
    public void testReadThroughGetBelowThresholdSucceeds() throws Exception {
        byte[] val = startGrid(0).getOrCreateCache(readThroughCacheConfig()).get(1);

        assertNotNull("Read-through load must succeed below threshold", val);
        assertEquals(ENTRY_SIZE, val.length);
    }

    /** @return Atomic read-through cache configuration. */
    private static CacheConfiguration<Integer, byte[]> readThroughCacheConfig() {
        return new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setReadThrough(true)
            .setCacheStoreFactory(new TestStoreFactory());
    }

    /**
     * Puts page-sized entries until the insert is rejected with an out-of-memory.
     *
     * @param ignite Node.
     * @param cache Cache.
     * @return Number of entries written before the out-of-memory.
     */
    private static int fillUntilOom(IgniteEx ignite, IgniteCache<Integer, byte[]> cache) throws IgniteCheckedException {
        byte[] val = new byte[ENTRY_SIZE];

        // One page per entry: the region can not hold as many entries as it has pages.
        long maxEntries = totalPages(ignite);

        for (int i = 0; i < maxEntries; i++) {
            try {
                cache.put(i, val);
            }
            catch (Exception e) {
                assertTrue("Expected IgniteOutOfMemoryException while filling the region, got: " + e,
                    X.hasCause(e, IgniteOutOfMemoryException.class));

                assertTrue(i > 0);

                return i;
            }
        }

        throw new AssertionError("Region was not filled up to the out-of-memory threshold");
    }
}
