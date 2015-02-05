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
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.swapspace.file.*;

import java.util.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CachePeekMode.*;

/**
 *
 */
public abstract class IgniteCacheSizeAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static final int HEAP_ENTRIES = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

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
    public void testSize() throws Exception {
        checkEmpty();

        if (cacheMode() == LOCAL) {
            IgniteCache<Integer, String> cache0 = jcache(0);

            IgniteCache<Integer, String> cacheAsync0 = cache0.withAsync();

            for (int i = 0; i < HEAP_ENTRIES; i++) {
                cache0.put(i, String.valueOf(i));

                final int size = i + 1;

                assertEquals(size, cache0.localSize());
                assertEquals(size, cache0.localSize(PRIMARY));
                assertEquals(size, cache0.localSize(BACKUP));
                assertEquals(size, cache0.localSize(NEAR));
                assertEquals(size, cache0.localSize(ALL));

                assertEquals(size, cache0.size());
                assertEquals(size, cache0.size(PRIMARY));
                assertEquals(size, cache0.size(BACKUP));
                assertEquals(size, cache0.size(NEAR));
                assertEquals(size, cache0.size(ALL));

                cacheAsync0.size();

                assertEquals(size, cacheAsync0.future().get());

                cacheAsync0.size(PRIMARY);

                assertEquals(size, cacheAsync0.future().get());
            }

            for (int i = 0; i < HEAP_ENTRIES; i++) {
                cache0.remove(i, String.valueOf(i));

                final int size = HEAP_ENTRIES - i - 1;

                assertEquals(size, cache0.localSize());
                assertEquals(size, cache0.localSize(PRIMARY));
                assertEquals(size, cache0.localSize(BACKUP));
                assertEquals(size, cache0.localSize(NEAR));
                assertEquals(size, cache0.localSize(ALL));

                assertEquals(size, cache0.size());
                assertEquals(size, cache0.size(PRIMARY));
                assertEquals(size, cache0.size(BACKUP));
                assertEquals(size, cache0.size(NEAR));
                assertEquals(size, cache0.size(ALL));

                cacheAsync0.size();

                assertEquals(size, cacheAsync0.future().get());
            }

            checkEmpty();

            for (int i = 0; i < 200; i++)
                cache0.put(i, "test_val");

            int totalKeys = 200;

            T2<Integer, Integer> swapKeys = swapKeys(0);

            T2<Integer, Integer> offheapKeys = offheapKeys(0);

            int totalSwap = swapKeys.get1() + swapKeys.get2();
            int totalOffheap = offheapKeys.get1() + offheapKeys.get2();

            log.info("Keys [total=" + totalKeys + ", offheap=" + offheapKeys + ", swap=" + swapKeys + ']');

            assertTrue(totalSwap + totalOffheap < totalKeys);

            assertEquals(totalKeys, cache0.localSize());
            assertEquals(totalKeys, cache0.localSize(ALL));

            assertEquals(totalOffheap, cache0.localSize(OFFHEAP));
            assertEquals(totalSwap, cache0.localSize(SWAP));
            assertEquals(totalKeys - (totalSwap + totalOffheap), cache0.localSize(ONHEAP));

            assertEquals(totalOffheap, cache0.size(OFFHEAP));
            assertEquals(totalSwap, cache0.size(SWAP));
            assertEquals(totalKeys - (totalSwap + totalOffheap), cache0.size(ONHEAP));

            assertEquals(totalOffheap, cache0.localSize(OFFHEAP, PRIMARY));
            assertEquals(totalSwap, cache0.localSize(SWAP, PRIMARY));
            assertEquals(totalKeys - (totalSwap + totalOffheap), cache0.localSize(ONHEAP, PRIMARY));

            assertEquals(totalOffheap, cache0.localSize(OFFHEAP, BACKUP));
            assertEquals(totalSwap, cache0.localSize(SWAP, BACKUP));
            assertEquals(totalKeys - (totalSwap + totalOffheap), cache0.localSize(ONHEAP, BACKUP));
        }
        else {
            checkSizeAffinityFilter(0);

            checkSizeAffinityFilter(1);

            checkSizeStorageFilter(0);

            checkSizeStorageFilter(1);
        }
    }

    /**
     * @param nodeIdx Node index.
     * @throws Exception If failed.
     */
    private void checkSizeAffinityFilter(int nodeIdx) throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(nodeIdx);

        final int PUT_KEYS = 10;

        List<Integer> keys = null;

        try {
            if (cacheMode() == REPLICATED) {
                keys = backupKeys(cache0, 10, 0);

                for (Integer key : keys)
                    cache0.put(key, String.valueOf(key));

                assertEquals(PUT_KEYS, cache0.localSize());
                assertEquals(PUT_KEYS, cache0.localSize(BACKUP));
                assertEquals(PUT_KEYS, cache0.localSize(ALL));
                assertEquals(0, cache0.localSize(PRIMARY));
                assertEquals(0, cache0.localSize(NEAR));

                for (int i = 0; i < gridCount(); i++) {
                    IgniteCache<Integer, String> cache = jcache(i);

                    assertEquals(0, cache.size(NEAR));
                    assertEquals(PUT_KEYS, cache.size(PRIMARY));
                    assertEquals(PUT_KEYS * (gridCount() - 1), cache.size(BACKUP));
                    assertEquals(PUT_KEYS * gridCount(), cache.size(PRIMARY, BACKUP));
                    assertEquals(PUT_KEYS * gridCount(), cache.size()); // Primary + backups.
                }
            }
            else {
                keys = nearKeys(cache0, PUT_KEYS, 0);

                for (Integer key : keys)
                    cache0.put(key, String.valueOf(key));

                boolean hasNearCache = nodeIdx == 0 ;

                if (hasNearCache) {
                    assertEquals(PUT_KEYS, cache0.localSize());
                    assertEquals(PUT_KEYS, cache0.localSize(ALL));
                    assertEquals(PUT_KEYS, cache0.localSize(NEAR));

                    for (int i = 0; i < gridCount(); i++) {
                        IgniteCache<Integer, String> cache = jcache(i);

                        assertEquals(PUT_KEYS, cache.size(NEAR));
                        assertEquals(PUT_KEYS, cache.size(BACKUP));
                        assertEquals(PUT_KEYS * 2, cache.size(PRIMARY, BACKUP));
                        assertEquals(PUT_KEYS * 2 + PUT_KEYS, cache.size()); // Primary + backups + near.
                    }
                }
                else {
                    assertEquals(0, cache0.localSize());
                    assertEquals(0, cache0.localSize(ALL));
                    assertEquals(0, cache0.localSize(NEAR));

                    for (int i = 0; i < gridCount(); i++) {
                        IgniteCache<Integer, String> cache = jcache(i);

                        assertEquals(0, cache.size(NEAR));
                        assertEquals(PUT_KEYS, cache.size(BACKUP));
                        assertEquals(PUT_KEYS * 2, cache.size(PRIMARY, BACKUP));
                        assertEquals(PUT_KEYS * 2, cache.size()); // Primary + backups.
                    }
                }

                assertEquals(0, cache0.localSize(BACKUP));
                assertEquals(0, cache0.localSize(PRIMARY));
            }

            checkPrimarySize(PUT_KEYS);

            CacheAffinity<Integer> aff = ignite(0).affinity(null);

            for (int i = 0; i < gridCount(); i++) {
                if (i == nodeIdx)
                    continue;

                ClusterNode node = ignite(i).cluster().localNode();

                int primary = 0;
                int backups = 0;

                for (Integer key : keys) {
                    if (aff.isPrimary(node, key))
                        primary++;
                    else if (aff.isBackup(node, key))
                        backups++;
                }

                IgniteCache<Integer, String> cache = jcache(i);

                assertEquals(primary, cache.localSize(PRIMARY));
                assertEquals(backups, cache.localSize(BACKUP));
                assertEquals(primary + backups, cache.localSize(PRIMARY, BACKUP));
                assertEquals(primary + backups, cache.localSize(BACKUP, PRIMARY));
                assertEquals(primary + backups, cache.localSize(ALL));
            }

            cache0.remove(keys.get(0));

            checkPrimarySize(PUT_KEYS - 1);

            if (cacheMode() == REPLICATED) {
                assertEquals(PUT_KEYS - 1, cache0.localSize());
                assertEquals(0, cache0.localSize(PRIMARY));
                assertEquals(PUT_KEYS - 1, cache0.localSize(BACKUP));
            }
            else {
                boolean hasNearCache = nodeIdx == 0;

                if (hasNearCache)
                    assertEquals(PUT_KEYS - 1, cache0.localSize());
                else
                    assertEquals(0, cache0.localSize());
            }
        }
        finally {
            if (keys != null)
                cache0.removeAll(new HashSet<>(keys));
        }

        checkEmpty();
    }

    /**
     * Checks size is zero.
     */
    private void checkEmpty() {
        for (int i = 0; i < gridCount(); i++) {
            IgniteCache<Integer, String> cache = jcache(i);

            assertEquals(0, cache.localSize());

            assertEquals(0, cache.size());

            for (CachePeekMode peekMode : CachePeekMode.values()) {
                assertEquals(0, cache.localSize(peekMode));

                assertEquals(0, cache.size(peekMode));
            }
        }

        checkPrimarySize(0);
    }

    /**
     * @param nodeIdx Node index.
     * @return Tuple with number of primary and backup keys.
     */
    private T2<Integer, Integer> swapKeys(int nodeIdx) {
        FileSwapSpaceSpi swap = (FileSwapSpaceSpi)ignite(nodeIdx).configuration().getSwapSpaceSpi();

        final String spaceName = "gg-swap-cache-dflt";

        IgniteSpiCloseableIterator<Integer> it = swap.keyIterator(spaceName, null);

        assertNotNull(it);

        CacheAffinity aff = ignite(nodeIdx).affinity(null);

        ClusterNode node = ignite(nodeIdx).cluster().localNode();

        int primary = 0;
        int backups = 0;

        while (it.hasNext()) {
            Integer key = it.next();

            if (aff.isPrimary(node, key))
                primary++;
            else {
                assertTrue(aff.isBackup(node, key));

                backups++;
            }
        }

        return new T2<>(primary, backups);
    }

    /**
     * @param nodeIdx Node index.
     * @return Tuple with number of primary and backup keys.
     */
    private T2<Integer, Integer> offheapKeys(int nodeIdx) {
        GridCacheAdapter<Integer, String> internalCache =
            ((IgniteKernal)ignite(nodeIdx)).context().cache().internalCache();

        Iterator<Map.Entry<Integer, String>> offheapIt;

        if (internalCache.context().isNear())
            offheapIt = internalCache.context().near().dht().context().swap().lazyOffHeapIterator();
        else
            offheapIt = internalCache.context().swap().lazyOffHeapIterator();

        CacheAffinity aff = ignite(nodeIdx).affinity(null);

        ClusterNode node = ignite(nodeIdx).cluster().localNode();

        int primary = 0;
        int backups = 0;

        while (offheapIt.hasNext()) {
            Map.Entry<Integer, String> e = offheapIt.next();

            if (aff.isPrimary(node, e.getKey()))
                primary++;
            else {
                assertTrue(aff.isBackup(node, e.getKey()));

                backups++;
            }
        }

        return new T2<>(primary, backups);
    }

    /**
     * @param nodeIdx Node index.
     * @throws Exception If failed.
     */
    private void checkSizeStorageFilter(int nodeIdx) throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(nodeIdx);

        List<Integer> primaryKeys = primaryKeys(cache0, 100, 10_000);
        List<Integer> backupKeys = backupKeys(cache0, 100, 10_000);

        try {
            final String val = "test_value";

            for (int i = 0; i < 100; i++) {
                cache0.put(primaryKeys.get(i), val);
                cache0.put(backupKeys.get(i), val);
            }

            int totalKeys = 200;

            T2<Integer, Integer> swapKeys = swapKeys(nodeIdx);

            assertTrue(swapKeys.get1() > 0);
            assertTrue(swapKeys.get2() > 0);

            T2<Integer, Integer> offheapKeys = offheapKeys(nodeIdx);

            assertTrue(offheapKeys.get1() > 0);
            assertTrue(offheapKeys.get2() > 0);

            int totalSwap = swapKeys.get1() + swapKeys.get2();
            int totalOffheap = offheapKeys.get1() + offheapKeys.get2();

            log.info("Keys [total=" + totalKeys + ", offheap=" + offheapKeys + ", swap=" + swapKeys + ']');

            assertTrue(totalSwap + totalOffheap < totalKeys);

            assertEquals(totalKeys, cache0.localSize());
            assertEquals(totalKeys, cache0.localSize(ALL));
            assertEquals(totalOffheap, cache0.localSize(OFFHEAP));
            assertEquals(totalSwap, cache0.localSize(SWAP));
            assertEquals(totalKeys - (totalOffheap + totalSwap), cache0.localSize(ONHEAP));
            assertEquals(totalKeys, cache0.localSize(SWAP, OFFHEAP, ONHEAP));

            assertEquals(swapKeys.get1(), (Integer)cache0.localSize(SWAP, PRIMARY));
            assertEquals(swapKeys.get2(), (Integer)cache0.localSize(SWAP, BACKUP));

            assertEquals(offheapKeys.get1(), (Integer)cache0.localSize(OFFHEAP, PRIMARY));
            assertEquals(offheapKeys.get2(), (Integer)cache0.localSize(OFFHEAP, BACKUP));

            assertEquals(swapKeys.get1() + offheapKeys.get1(), cache0.localSize(SWAP, OFFHEAP, PRIMARY));
            assertEquals(swapKeys.get2() + offheapKeys.get2(), cache0.localSize(SWAP, OFFHEAP, BACKUP));

            assertEquals(totalSwap + totalOffheap, cache0.localSize(SWAP, OFFHEAP));
        }
        finally {
            cache0.removeAll(new HashSet<>(primaryKeys));
            cache0.removeAll(new HashSet<>(backupKeys));
        }

        checkEmpty();
    }

    /**
     * @param exp Expected size.
     */
    private void checkPrimarySize(int exp) {
        int size = 0;

        for (int i = 0; i < gridCount(); i++) {
            IgniteCache<Integer, String> cache = jcache(i);

            IgniteCache<Integer, String> cacheAsync = cache.withAsync();

            assertEquals(exp, cache.size(PRIMARY));

            size += cache.localSize(PRIMARY);

            cacheAsync.size(PRIMARY);

            assertEquals(exp, cacheAsync.future().get());
        }

        assertEquals(exp, size);
    }
}
