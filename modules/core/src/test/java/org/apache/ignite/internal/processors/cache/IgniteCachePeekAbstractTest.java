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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.eviction.fifo.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.swapspace.inmemory.*;

import java.util.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CachePeekMode.*;

/**
 *
 */
public abstract class IgniteCachePeekAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static final int HEAP_ENTRIES = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);

        ccfg.setOffHeapMaxMemory(512);

        ccfg.setBackups(1);

        if (gridName.equals(getTestGridName(0)))
            ccfg.setDistributionMode(NEAR_PARTITIONED);

        ccfg.setEvictionPolicy(new CacheFifoEvictionPolicy(HEAP_ENTRIES));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalPeek() throws Exception {
        checkStorage();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinity() throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(0);

        Integer key = nearKey(cache0);

        final String val = "1";

        cache0.put(key, val);

        assertEquals(val, cache(0).peek(key));
        assertEquals(val, cache0.localPeek(key, NEAR));
        assertEquals(val, cache0.localPeek(key, ALL));
        assertNull(cache0.localPeek(key, PRIMARY));
        assertNull(cache0.localPeek(key, BACKUP));

        CacheAffinity<Integer> aff = ignite(0).affinity(null);

        for (int i = 1; i < gridCount(); i++) {
            IgniteCache<Integer, String> cache = jcache(i);

            assertNull(cache.localPeek(key, NEAR));

            if (aff.isPrimary(ignite(i).cluster().localNode(), key)) {
                assertEquals(val, cache.localPeek(key, PRIMARY));
                assertEquals(val, cache.localPeek(key, ALL));
                assertNull(cache.localPeek(key, BACKUP));
                assertNull(cache.localPeek(key, NEAR));
            }
            else if (aff.isBackup(ignite(i).cluster().localNode(), key)) {
                assertEquals(val, cache.localPeek(key, BACKUP));
                assertEquals(val, cache.localPeek(key, ALL));
                assertNull(cache.localPeek(key, PRIMARY));
                assertNull(cache.localPeek(key, NEAR));
            }
            else {
                assertNull(cache.localPeek(key, ALL));
                assertNull(cache.localPeek(key, PRIMARY));
                assertNull(cache.localPeek(key, BACKUP));
                assertNull(cache.localPeek(key, NEAR));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkStorage() throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(0);

        List<Integer> keys = primaryKeys(cache0, 100, 10_000);

        final String val = "test_value";

        for (Integer key : keys)
            cache0.put(key, val);

        GridTestSwapSpaceSpi swap = (GridTestSwapSpaceSpi)ignite(0).configuration().getSwapSpaceSpi();

        Set<Integer> swapKeys = new HashSet<>();

        final String spaceName = "gg-swap-cache-dflt";

        IgniteSpiCloseableIterator<Integer> it = swap.keyIterator(spaceName, null);

        assertNotNull(it);

        while (it.hasNext())
            assertTrue(swapKeys.add(it.next()));

        assertFalse(swapKeys.isEmpty());

        assertTrue(swapKeys.size() + HEAP_ENTRIES < 100);

        List<Integer> offheapKeys = new ArrayList<>(keys);

        for (Integer key : swapKeys) {
            assertEquals(val, cache0.localPeek(key, SWAP));

            assertNull(cache0.localPeek(key, ONHEAP));
            assertNull(cache0.localPeek(key, OFFHEAP));

            offheapKeys.remove(key);
        }

        for (int i = 0; i < HEAP_ENTRIES; i++) {
            Integer key = keys.get(keys.size() - i - 1);

            assertFalse(swapKeys.contains(key));
            assertEquals(val, cache0.localPeek(key, ONHEAP));

            assertNull(cache0.localPeek(key, SWAP));
            assertNull(cache0.localPeek(key, OFFHEAP));

            offheapKeys.remove(key);
        }
    }
}