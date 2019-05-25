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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class PageEvictionTouchOrderTest extends PageEvictionAbstractTest {
    /** Test entries number. */
    private static final int SAFE_ENTRIES = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(/* Overriden by FairFifoPageEvictionTracker */DataPageEvictionMode.RANDOM_LRU,
            super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty("override.fair.fifo.page.eviction.tracker", "true");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTouchOrderWithFairFifoEvictionAtomicReplicated() throws Exception {
        testTouchOrderWithFairFifoEviction(CacheAtomicityMode.ATOMIC, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTouchOrderWithFairFifoEvictionAtomicLocal() throws Exception {
        testTouchOrderWithFairFifoEviction(CacheAtomicityMode.ATOMIC, CacheMode.LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTouchOrderWithFairFifoEvictionTxReplicated() throws Exception {
        testTouchOrderWithFairFifoEviction(CacheAtomicityMode.TRANSACTIONAL, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTouchOrderWithFairFifoEvictionTxLocal() throws Exception {
        testTouchOrderWithFairFifoEviction(CacheAtomicityMode.TRANSACTIONAL, CacheMode.LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10738,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testTouchOrderWithFairFifoEvictionMvccTxReplicated() throws Exception {
        testTouchOrderWithFairFifoEviction(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10738,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testTouchOrderWithFairFifoEvictionMvccTxPartitioned() throws Exception {
        testTouchOrderWithFairFifoEviction(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7956,https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testTouchOrderWithFairFifoEvictionMvccTxLocal() throws Exception {
        testTouchOrderWithFairFifoEviction(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.LOCAL);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param cacheMode Cache mode.
     * @throws Exception If failed.
     */
    private void testTouchOrderWithFairFifoEviction(CacheAtomicityMode atomicityMode, CacheMode cacheMode)
        throws Exception {
        startGrid(0);

        CacheConfiguration<Object, Object> cfg = cacheConfig("evict-fair", null, cacheMode, atomicityMode,
            CacheWriteSynchronizationMode.PRIMARY_SYNC);

        IgniteCache<Object, Object> cache = ignite(0).getOrCreateCache(cfg);

        for (int i = 1; i <= ENTRIES; i++) {
            cache.put(i, new TestObject(PAGE_SIZE / 6));
            // Row size is between PAGE_SIZE / 2 and PAGE_SIZE. Enforces "one row - one page".

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries put: " + i);
        }

        for (int i = ENTRIES - SAFE_ENTRIES + 1; i <= ENTRIES; i++)
            assertNotNull(cache.get(i));

        for (int i = 1; i <= SAFE_ENTRIES; i++)
            assertNull(cache.get(i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.setProperty("override.fair.fifo.page.eviction.tracker", "false");
    }

}
