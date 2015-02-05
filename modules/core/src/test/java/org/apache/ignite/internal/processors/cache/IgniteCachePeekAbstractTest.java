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
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.swapspace.inmemory.*;

import java.util.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
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
        if (cacheMode() == LOCAL) {
            checkAffinityLocalCache();

            checkStorage(0);
        }
        else {
            checkAffinityPeek(0);

            checkAffinityPeek(1);

            checkStorage(0);

            checkStorage(1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinityLocalCache() throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(0);

        final String val = "1";

        for (int i = 0; i < HEAP_ENTRIES; i++)
            cache0.put(i, val);

        try {
            for (int i = 0; i < HEAP_ENTRIES; i++) {
                assertEquals(val, cache0.localPeek(i));
                assertEquals(val, cache0.localPeek(i, ALL));
                assertEquals(val, cache0.localPeek(i, PRIMARY));
                assertEquals(val, cache0.localPeek(i, BACKUP));
                assertEquals(val, cache0.localPeek(i, NEAR));
            }
        }
        finally {
            for (int i = 0; i < HEAP_ENTRIES; i++)
                cache0.remove(i);
        }
    }

    /**
     * @param nodeIdx Node index.
     * @throws Exception If failed.
     */
    private void checkAffinityPeek(int nodeIdx) throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(nodeIdx);

        final String val = "1";

        Integer key = null;

        try {
            if (cacheMode() == REPLICATED) {
                key = backupKey(cache0);

                cache0.put(key, val);

                assertEquals(val, cache0.localPeek(key, ALL));
                assertEquals(val, cache0.localPeek(key, BACKUP));
                assertNull(cache0.localPeek(key, NEAR));
                assertNull(cache0.localPeek(key, PRIMARY));
            }
            else {
                key = nearKey(cache0);

                cache0.put(key, val);

                boolean hasNearCache = nodeIdx == 0 ;

                if (hasNearCache) {
                    assertEquals(val, cache0.localPeek(key, NEAR));
                    assertEquals(val, cache0.localPeek(key, ALL));
                }
                else {
                    assertNull(cache0.localPeek(key, NEAR));
                    assertNull(cache0.localPeek(key, ALL));
                }

                assertNull(cache0.localPeek(key, PRIMARY));
                assertNull(cache0.localPeek(key, BACKUP));
            }

            CacheAffinity<Integer> aff = ignite(0).affinity(null);

            for (int i = 0; i < gridCount(); i++) {
                if (i == nodeIdx)
                    continue;

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
        finally {
            if (key != null)
                cache0.remove(key);
        }
    }

    /**
     * @param nodeIdx Node index.
     * @throws Exception If failed.
     */
    private void checkStorage(int nodeIdx) throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(nodeIdx);

        List<Integer> keys = primaryKeys(cache0, 100, 10_000);

        try {
            final String val = "test_value";

            for (Integer key : keys)
                cache0.put(key, val);

            GridTestSwapSpaceSpi swap = (GridTestSwapSpaceSpi)ignite(nodeIdx).configuration().getSwapSpaceSpi();

            Set<Integer> swapKeys = new HashSet<>();

            final String spaceName = "gg-swap-cache-dflt";

            IgniteSpiCloseableIterator<Integer> it = swap.keyIterator(spaceName, null);

            assertNotNull(it);

            while (it.hasNext())
                assertTrue(swapKeys.add(it.next()));

            assertFalse(swapKeys.isEmpty());

            assertTrue(swapKeys.size() + HEAP_ENTRIES < 100);

            Set<Integer> offheapKeys = new HashSet<>();

            GridCacheAdapter<Integer, String> internalCache =
                ((IgniteKernal)ignite(nodeIdx)).context().cache().internalCache();

            Iterator<Map.Entry<Integer, String>> offheapIt;

            if (internalCache.context().isNear())
                offheapIt = internalCache.context().near().dht().context().swap().lazyOffHeapIterator();
            else
                offheapIt = internalCache.context().swap().lazyOffHeapIterator();

            while (offheapIt.hasNext()) {
                Map.Entry<Integer, String> e = offheapIt.next();

                assertTrue(offheapKeys.add(e.getKey()));

                assertFalse(swapKeys.contains(e.getKey()));
            }

            assertFalse(offheapKeys.isEmpty());

            Set<Integer> heapKeys = new HashSet<>(keys);

            heapKeys.removeAll(offheapKeys);
            heapKeys.removeAll(swapKeys);

            assertFalse(heapKeys.isEmpty());

            log.info("Keys [swap=" + swapKeys.size() +
                ", offheap=" + offheapKeys.size() +
                ", heap=" + heapKeys.size() + ']');

            assertEquals(100, swapKeys.size() + offheapKeys.size() + heapKeys.size());

            for (Integer key : swapKeys) {
                assertEquals(val, cache0.localPeek(key, SWAP));

                assertNull(cache0.localPeek(key, ONHEAP));
                assertNull(cache0.localPeek(key, OFFHEAP));
            }

            for (Integer key : offheapKeys) {
                assertEquals(val, cache0.localPeek(key, OFFHEAP));

                assertNull(cache0.localPeek(key, ONHEAP));
                assertNull(cache0.localPeek(key, SWAP));
            }

            for (Integer key : heapKeys) {
                assertEquals(val, cache0.localPeek(key, ONHEAP));

                assertNull(cache0.localPeek(key, SWAP));
                assertNull(cache0.localPeek(key, OFFHEAP));
            }
        }
        finally {
            cache0.removeAll(new HashSet<>(keys));
        }
    }
}
