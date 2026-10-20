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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Regression test for the disabled-eviction in-memory OOM guard on insert paths that bypass {@code FreeList.reserve}:
 * read-through inside {@code get()} runs under the entry lock and cannot reserve, so the per-insert guard fire and reject the insert
 * with {@link IgniteOutOfMemoryException} when the region is at the threshold — rather than silently consuming the
 * free-list bookkeeping margin.
 */
public class PageEvictionDisabledEvictionReadThroughOomSelfTest extends PageEvictionAbstractTest {
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
    @Test(timeout = 120_000)
    public void testReadThroughGetOomAtThreshold() throws Exception {
        IgniteEx ignite = startGrid(0);

        CacheConfiguration<Integer, byte[]> cfg = new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setReadThrough(true)
            .setCacheStoreFactory(new TestStoreFactory());

        IgniteCache<Integer, byte[]> cache = grid(0).getOrCreateCache(cfg);

        byte[] small = new byte[pageSize(ignite) - 200];

        int filled = 0;

        for (int i = 0; i < 100_000; i++) {
            try {
                cache.put(i, small);

                filled++;
            }
            catch (Exception e) {
                assertTrue("Expected IgniteOutOfMemoryException while filling the region, got: " + e,
                    X.hasCause(e, IgniteOutOfMemoryException.class));

                break;
            }
        }

        assertTrue(filled > 0);

        int missingKey = filled + 1;

        GridTestUtils.assertThrowsWithCause(() -> cache.get(missingKey), IgniteOutOfMemoryException.class);
    }

    /** */
    @Test
    public void testReadThroughGetBelowThresholdSucceeds() throws Exception {
        startGrid(0);

        CacheConfiguration<Integer, byte[]> cfg = new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setReadThrough(true)
            .setCacheStoreFactory(new TestStoreFactory());

        byte[] val = grid(0).getOrCreateCache(cfg).get(1);

        assertNotNull("Read-through load must succeed below threshold", val);
        assertEquals(PAGE_SIZE - 200, val.length);
    }
}
