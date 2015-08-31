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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.ALL;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;
import static org.apache.ignite.cache.CachePeekMode.NEAR;
import static org.apache.ignite.cache.CachePeekMode.OFFHEAP;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.cache.CachePeekMode.SWAP;

/**
 * Tests for methods using {@link CachePeekMode}:
 * <ul>
 *     <li>{@link IgniteCache#localPeek(Object, CachePeekMode...)}</li>
 *     <li>{@link IgniteCache#localSize(CachePeekMode...)}</li>
 *     <li>{@link IgniteCache#size(CachePeekMode...)}</li>
 *     <li>{@link IgniteCache#localEntries(CachePeekMode...)}</li>
 * </ul>
 */
public abstract class IgniteCachePeekModesAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static final String SPACE_NAME = "gg-swap-cache-dflt";

    /** */
    private static final int HEAP_ENTRIES = 30;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setSwapSpaceSpi(spi());

        return cfg;
    }

    /**
     * Creates a SwapSpaceSpi.
     * @return the Spi
     */
    protected SwapSpaceSpi spi() {
        return new FileSwapSpaceSpi();
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @return Has near cache flag.
     */
    protected boolean hasNearCache() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);

        ccfg.setOffHeapMaxMemory(512);

        ccfg.setBackups(1);

        if (hasNearCache())
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(HEAP_ENTRIES);

        ccfg.setEvictionPolicy(plc);

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

                if (hasNearCache()) {
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

            Affinity<Integer> aff = ignite(0).affinity(null);

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

            SwapSpaceSpi swap = ignite(nodeIdx).configuration().getSwapSpaceSpi();

            Set<Integer> swapKeys = new HashSet<>();

            IgniteSpiCloseableIterator<Integer> it = swap.keyIterator(SPACE_NAME, null);

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
                assertEquals(val, cache0.localPeek(key, PRIMARY, SWAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP, SWAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP, OFFHEAP, SWAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, ONHEAP, SWAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, ONHEAP, OFFHEAP, SWAP));

                if (cacheMode() == LOCAL) {
                    assertEquals(val, cache0.localPeek(key, SWAP, BACKUP));
                    assertEquals(val, cache0.localPeek(key, SWAP, NEAR));
                }
                else {
                    assertNull(cache0.localPeek(key, SWAP, BACKUP));
                    assertNull(cache0.localPeek(key, SWAP, NEAR));
                }

                assertNull(cache0.localPeek(key, ONHEAP));
                assertNull(cache0.localPeek(key, OFFHEAP));
            }

            for (Integer key : offheapKeys) {
                assertEquals(val, cache0.localPeek(key, OFFHEAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP, OFFHEAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP, SWAP, OFFHEAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, OFFHEAP));

                if (cacheMode() == LOCAL) {
                    assertEquals(val, cache0.localPeek(key, OFFHEAP, BACKUP));
                    assertEquals(val, cache0.localPeek(key, OFFHEAP, NEAR));
                }
                else {
                    assertNull(cache0.localPeek(key, OFFHEAP, BACKUP));
                    assertNull(cache0.localPeek(key, OFFHEAP, NEAR));
                }

                assertNull(cache0.localPeek(key, ONHEAP));
                assertNull(cache0.localPeek(key, SWAP));
            }

            for (Integer key : heapKeys) {
                assertEquals(val, cache0.localPeek(key, ONHEAP));
                assertEquals(val, cache0.localPeek(key, SWAP, ONHEAP));
                assertEquals(val, cache0.localPeek(key, SWAP, OFFHEAP, ONHEAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, ONHEAP));

                if (cacheMode() == LOCAL) {
                    assertEquals(val, cache0.localPeek(key, ONHEAP, BACKUP));
                    assertEquals(val, cache0.localPeek(key, ONHEAP, NEAR));
                }
                else {
                    assertNull(cache0.localPeek(key, ONHEAP, BACKUP));
                    assertNull(cache0.localPeek(key, ONHEAP, NEAR));
                }

                assertNull(cache0.localPeek(key, SWAP));
                assertNull(cache0.localPeek(key, OFFHEAP));
            }
        }
        finally {
            cache0.removeAll(new HashSet<>(keys));
        }
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

            Set<Integer> keys = new HashSet<>();

            for (int i = 0; i < 200; i++) {
                cache0.put(i, "test_val");

                keys.add(i);
            }

            try {
                int totalKeys = 200;

                T2<Integer, Integer> swapKeys = swapKeysCount(0);

                T2<Integer, Integer> offheapKeys = offheapKeysCount(0);

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
            finally {
                cache0.removeAll(keys);
            }
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

                assertEquals(PUT_KEYS, cache0.localSize(BACKUP));
                assertEquals(PUT_KEYS, cache0.localSize(ALL));
                assertEquals(0, cache0.localSize());
                assertEquals(0, cache0.localSize(PRIMARY));
                assertEquals(0, cache0.localSize(NEAR));

                for (int i = 0; i < gridCount(); i++) {
                    IgniteCache<Integer, String> cache = jcache(i);

                    assertEquals(0, cache.size(NEAR));
                    assertEquals(PUT_KEYS, cache.size(PRIMARY));
                    assertEquals(PUT_KEYS * (gridCount() - 1), cache.size(BACKUP));
                    assertEquals(PUT_KEYS * gridCount(), cache.size(PRIMARY, BACKUP));
                    assertEquals(PUT_KEYS * gridCount(), cache.size(ALL)); // Primary + backups.
                }
            }
            else {
                keys = nearKeys(cache0, PUT_KEYS, 0);

                for (Integer key : keys)
                    cache0.put(key, String.valueOf(key));

                if (hasNearCache()) {
                    assertEquals(0, cache0.localSize());
                    assertEquals(PUT_KEYS, cache0.localSize(ALL));
                    assertEquals(PUT_KEYS, cache0.localSize(NEAR));

                    for (int i = 0; i < gridCount(); i++) {
                        IgniteCache<Integer, String> cache = jcache(i);

                        assertEquals(PUT_KEYS, cache.size(NEAR));
                        assertEquals(PUT_KEYS, cache.size(BACKUP));
                        assertEquals(PUT_KEYS * 2, cache.size(PRIMARY, BACKUP));
                        assertEquals(PUT_KEYS * 2 + PUT_KEYS, cache.size(ALL)); // Primary + backups + near.
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
                        assertEquals(PUT_KEYS * 2, cache.size(ALL)); // Primary + backups.
                    }
                }

                assertEquals(0, cache0.localSize(BACKUP));
                assertEquals(0, cache0.localSize(PRIMARY));
            }

            checkPrimarySize(PUT_KEYS);

            Affinity<Integer> aff = ignite(0).affinity(null);

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
                assertEquals(PUT_KEYS - 1, cache0.localSize(ALL));
                assertEquals(0, cache0.localSize(PRIMARY));
                assertEquals(PUT_KEYS - 1, cache0.localSize(BACKUP));
            }
            else {
                if (hasNearCache())
                    assertEquals(PUT_KEYS - 1, cache0.localSize(ALL));
                else
                    assertEquals(0, cache0.localSize(ALL));
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
     * @return Tuple with primary and backup keys.
     */
    private T2<List<Integer>, List<Integer>> swapKeys(int nodeIdx) {
        SwapSpaceSpi swap = ignite(nodeIdx).configuration().getSwapSpaceSpi();

        IgniteSpiCloseableIterator<Integer> it = swap.keyIterator(SPACE_NAME, null);

        assertNotNull(it);

        Affinity aff = ignite(nodeIdx).affinity(null);

        ClusterNode node = ignite(nodeIdx).cluster().localNode();

        List<Integer> primary = new ArrayList<>();
        List<Integer> backups = new ArrayList<>();

        while (it.hasNext()) {
            Integer key = it.next();

            if (aff.isPrimary(node, key))
                primary.add(key);
            else {
                assertTrue(aff.isBackup(node, key));

                backups.add(key);
            }
        }

        return new T2<>(primary, backups);
    }

    /**
     * @param nodeIdx Node index.
     * @return Tuple with number of primary and backup keys.
     */
    private T2<Integer, Integer> swapKeysCount(int nodeIdx) {
        T2<List<Integer>, List<Integer>> keys = swapKeys(nodeIdx);

        return new T2<>(keys.get1().size(), keys.get2().size());
    }

    /**
     * @param nodeIdx Node index.
     * @return Tuple with primary and backup keys.
     */
    private T2<List<Integer>, List<Integer>> offheapKeys(int nodeIdx) {
        GridCacheAdapter<Integer, String> internalCache =
            ((IgniteKernal)ignite(nodeIdx)).context().cache().internalCache();

        Iterator<Map.Entry<Integer, String>> offheapIt;

        if (internalCache.context().isNear())
            offheapIt = internalCache.context().near().dht().context().swap().lazyOffHeapIterator();
        else
            offheapIt = internalCache.context().swap().lazyOffHeapIterator();

        Affinity aff = ignite(nodeIdx).affinity(null);

        ClusterNode node = ignite(nodeIdx).cluster().localNode();

        List<Integer> primary = new ArrayList<>();
        List<Integer> backups = new ArrayList<>();

        while (offheapIt.hasNext()) {
            Map.Entry<Integer, String> e = offheapIt.next();

            if (aff.isPrimary(node, e.getKey()))
                primary.add(e.getKey());
            else {
                assertTrue(aff.isBackup(node, e.getKey()));

                backups.add(e.getKey());
            }
        }

        return new T2<>(primary, backups);
    }

    /**
     * @param nodeIdx Node index.
     * @return Tuple with number of primary and backup keys.
     */
    private T2<Integer, Integer> offheapKeysCount(int nodeIdx) {
        T2<List<Integer>, List<Integer>> keys = offheapKeys(nodeIdx);

        return new T2<>(keys.get1().size(), keys.get2().size());
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

            T2<Integer, Integer> swapKeys = swapKeysCount(nodeIdx);

            assertTrue(swapKeys.get1() > 0);
            assertTrue(swapKeys.get2() > 0);

            T2<Integer, Integer> offheapKeys = offheapKeysCount(nodeIdx);

            assertTrue(offheapKeys.get1() > 0);
            assertTrue(offheapKeys.get2() > 0);

            int totalSwap = swapKeys.get1() + swapKeys.get2();
            int totalOffheap = offheapKeys.get1() + offheapKeys.get2();

            log.info("Local keys [total=" + totalKeys + ", offheap=" + offheapKeys + ", swap=" + swapKeys + ']');

            assertTrue(totalSwap + totalOffheap < totalKeys);

            assertEquals(primaryKeys.size(), cache0.localSize());
            assertEquals(totalKeys, cache0.localSize(ALL));
            assertEquals(totalOffheap, cache0.localSize(PRIMARY, BACKUP, NEAR, OFFHEAP));
            assertEquals(totalSwap, cache0.localSize(PRIMARY, BACKUP, NEAR, SWAP));
            assertEquals(totalKeys - (totalOffheap + totalSwap), cache0.localSize(PRIMARY, BACKUP, NEAR, ONHEAP));
            assertEquals(totalKeys, cache0.localSize(PRIMARY, BACKUP, NEAR, SWAP, OFFHEAP, ONHEAP));

            assertEquals(swapKeys.get1(), (Integer)cache0.localSize(SWAP, PRIMARY));
            assertEquals(swapKeys.get2(), (Integer)cache0.localSize(SWAP, BACKUP));

            assertEquals(offheapKeys.get1(), (Integer)cache0.localSize(OFFHEAP, PRIMARY));
            assertEquals(offheapKeys.get2(), (Integer)cache0.localSize(OFFHEAP, BACKUP));

            assertEquals(swapKeys.get1() + offheapKeys.get1(), cache0.localSize(SWAP, OFFHEAP, PRIMARY));
            assertEquals(swapKeys.get2() + offheapKeys.get2(), cache0.localSize(SWAP, OFFHEAP, BACKUP));

            assertEquals(totalSwap + totalOffheap, cache0.localSize(PRIMARY, BACKUP, NEAR, SWAP, OFFHEAP));

            int globalSwapPrimary = 0;
            int globalSwapBackup = 0;

            int globalOffheapPrimary = 0;
            int globalOffheapBackup = 0;

            for (int i = 0; i < gridCount(); i++) {
                T2<Integer, Integer> swap = swapKeysCount(i);

                globalSwapPrimary += swap.get1();
                globalSwapBackup += swap.get2();

                T2<Integer, Integer> offheap = offheapKeysCount(i);

                globalOffheapPrimary += offheap.get1();
                globalOffheapBackup += offheap.get2();
            }

            int backups;

            if (cacheMode() == LOCAL)
                backups = 0;
            else if (cacheMode() == PARTITIONED)
                backups = 1;
            else // REPLICATED.
                backups = gridCount() - 1;

            int globalTotal = totalKeys + totalKeys * backups;
            int globalTotalSwap = globalSwapPrimary + globalSwapBackup;
            int globalTotalOffheap = globalOffheapPrimary + globalOffheapBackup;

            log.info("Global keys [total=" + globalTotal +
                ", offheap=" + globalTotalOffheap +
                ", swap=" + globalTotalSwap + ']');

            for (int i = 0; i < gridCount(); i++) {
                IgniteCache<Integer, String> cache = jcache(i);

                assertEquals(totalKeys, cache.size(PRIMARY));
                assertEquals(globalTotal, cache.size(ALL));
                assertEquals(globalTotal, cache.size(PRIMARY, BACKUP, NEAR, ONHEAP, OFFHEAP, SWAP));
                assertEquals(globalTotal, cache.size(ONHEAP, OFFHEAP, SWAP, PRIMARY, BACKUP));

                assertEquals(globalTotalSwap, cache.size(PRIMARY, BACKUP, NEAR, SWAP));
                assertEquals(globalSwapPrimary, cache.size(SWAP, PRIMARY));
                assertEquals(globalSwapBackup, cache.size(SWAP, BACKUP));

                assertEquals(globalTotalOffheap, cache.size(PRIMARY, BACKUP, NEAR, OFFHEAP));
                assertEquals(globalOffheapPrimary, cache.size(OFFHEAP, PRIMARY));
                assertEquals(globalOffheapBackup, cache.size(OFFHEAP, BACKUP));

                assertEquals(globalTotalSwap + globalTotalOffheap, cache.size(PRIMARY, BACKUP, NEAR, SWAP, OFFHEAP));
                assertEquals(globalSwapPrimary + globalOffheapPrimary, cache.size(SWAP, OFFHEAP, PRIMARY));
                assertEquals(globalSwapBackup + globalOffheapBackup, cache.size(SWAP, OFFHEAP, BACKUP));

                assertEquals(globalTotal - (globalTotalOffheap + globalTotalSwap), cache.size(PRIMARY, BACKUP, NEAR, ONHEAP));
            }
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

    /**
     * @throws Exception If failed.
     */
    public void testLocalEntries() throws Exception {
        if (cacheMode() == LOCAL) {
            IgniteCache<Integer, String> cache0 = jcache(0);

            Set<Integer> keys = new HashSet<>();

            try {
                for (int i = 0; i < HEAP_ENTRIES; i++) {
                    cache0.put(i, String.valueOf(i));

                    keys.add(i);
                }

                checkLocalEntries(cache0.localEntries(), keys);
                checkLocalEntries(cache0.localEntries(ALL), keys);
                checkLocalEntries(cache0.localEntries(NEAR), keys);
                checkLocalEntries(cache0.localEntries(PRIMARY), keys);
                checkLocalEntries(cache0.localEntries(BACKUP), keys);
            }
            finally {
                cache0.removeAll(keys);
            }

            checkLocalEntries(cache0.localEntries());

            final String val = "test-val-";

            keys = new HashSet<>();

            for (int i = 0; i < 200; i++) {
                cache0.put(i, val + i);

                keys.add(i);
            }

            try {
                int totalKeys = 200;

                T2<List<Integer>, List<Integer>> swapKeys = swapKeys(0);

                T2<List<Integer>, List<Integer>> offheapKeys = offheapKeys(0);

                List<Integer> swap = new ArrayList<>();

                swap.addAll(swapKeys.get1());
                swap.addAll(swapKeys.get2());

                assertFalse(swap.isEmpty());

                List<Integer> offheap = new ArrayList<>();

                offheap.addAll(offheapKeys.get1());
                offheap.addAll(offheapKeys.get2());

                assertFalse(offheap.isEmpty());

                log.info("Keys [total=" + totalKeys +
                    ", offheap=" + offheap.size() +
                    ", swap=" + swap.size() + ']');

                assertTrue(swap.size() + offheap.size() < totalKeys);

                List<Integer> heap = new ArrayList<>(keys);

                heap.removeAll(swap);
                heap.removeAll(offheap);

                assertFalse(heap.isEmpty());

                checkLocalEntries(cache0.localEntries(), val, keys);
                checkLocalEntries(cache0.localEntries(ALL), val, keys);

                checkLocalEntries(cache0.localEntries(OFFHEAP), val, offheap);
                checkLocalEntries(cache0.localEntries(SWAP), val, swap);
                checkLocalEntries(cache0.localEntries(ONHEAP), val, heap);

                checkLocalEntries(cache0.localEntries(OFFHEAP, PRIMARY), val, offheap);
                checkLocalEntries(cache0.localEntries(SWAP, PRIMARY), val, swap);
                checkLocalEntries(cache0.localEntries(ONHEAP, PRIMARY), val, heap);

                checkLocalEntries(cache0.localEntries(OFFHEAP, BACKUP), val, offheap);
                checkLocalEntries(cache0.localEntries(SWAP, BACKUP), val, swap);
                checkLocalEntries(cache0.localEntries(ONHEAP, BACKUP), val, heap);

                checkLocalEntries(cache0.localEntries(OFFHEAP, NEAR), val, offheap);
                checkLocalEntries(cache0.localEntries(SWAP, NEAR), val, swap);
                checkLocalEntries(cache0.localEntries(ONHEAP, NEAR), val, heap);
            }
            finally {
                cache0.removeAll(keys);
            }
        }
        else {
            checkLocalEntriesAffinityFilter(0);

            checkLocalEntriesAffinityFilter(1);

            checkLocalEntriesStorageFilter(0);

            checkLocalEntriesStorageFilter(1);
        }
    }

    /**
     * @param nodeIdx Node index.
     * @throws Exception If failed.
     */
    private void checkLocalEntriesStorageFilter(int nodeIdx) throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(nodeIdx);

        List<Integer> primaryKeys = primaryKeys(cache0, 100, 10_000);
        List<Integer> backupKeys = backupKeys(cache0, 100, 10_000);

        try {
            final String val = "test_value-";

            for (int i = 0; i < 100; i++) {
                cache0.put(primaryKeys.get(i), val + primaryKeys.get(i));
                cache0.put(backupKeys.get(i), val + backupKeys.get(i));
            }

            int totalKeys = 200;

            T2<List<Integer>, List<Integer>> swapKeys = swapKeys(nodeIdx);

            assertTrue(swapKeys.get1().size() > 0);
            assertTrue(swapKeys.get2().size() > 0);

            T2<List<Integer>, List<Integer>> offheapKeys = offheapKeys(nodeIdx);

            assertTrue(offheapKeys.get1().size() > 0);
            assertTrue(offheapKeys.get2().size() > 0);

            List<Integer> swap = new ArrayList<>();

            swap.addAll(swapKeys.get1());
            swap.addAll(swapKeys.get2());

            assertFalse(swap.isEmpty());

            List<Integer> offheap = new ArrayList<>();

            offheap.addAll(offheapKeys.get1());
            offheap.addAll(offheapKeys.get2());

            assertFalse(offheap.isEmpty());

            List<Integer> heap = new ArrayList<>();

            heap.addAll(primaryKeys);
            heap.addAll(backupKeys);

            heap.removeAll(swap);
            heap.removeAll(offheap);

            log.info("Keys [total=" + totalKeys +
                ", offheap=" + offheap.size() +
                ", swap=" + swap.size() + ']');

            assertFalse(heap.isEmpty());

            checkLocalEntries(cache0.localEntries(), val, primaryKeys, backupKeys);
            checkLocalEntries(cache0.localEntries(ALL), val, primaryKeys, backupKeys);
            checkLocalEntries(cache0.localEntries(ONHEAP, OFFHEAP, SWAP), val, primaryKeys, backupKeys);

            checkLocalEntries(cache0.localEntries(SWAP), val, swap);
            checkLocalEntries(cache0.localEntries(OFFHEAP), val, offheap);
            checkLocalEntries(cache0.localEntries(ONHEAP), val, heap);

            checkLocalEntries(cache0.localEntries(SWAP, OFFHEAP), val, swap, offheap);
            checkLocalEntries(cache0.localEntries(SWAP, ONHEAP), val, swap, heap);

            checkLocalEntries(cache0.localEntries(SWAP, PRIMARY), val, swapKeys.get1());
            checkLocalEntries(cache0.localEntries(SWAP, BACKUP), val, swapKeys.get2());
            checkLocalEntries(cache0.localEntries(OFFHEAP, PRIMARY), val, offheapKeys.get1());
            checkLocalEntries(cache0.localEntries(OFFHEAP, BACKUP), val, offheapKeys.get2());

            checkLocalEntries(cache0.localEntries(SWAP, OFFHEAP, PRIMARY), val, swapKeys.get1(), offheapKeys.get1());
            checkLocalEntries(cache0.localEntries(SWAP, OFFHEAP, BACKUP), val, swapKeys.get2(), offheapKeys.get2());
            checkLocalEntries(cache0.localEntries(SWAP, OFFHEAP, PRIMARY, BACKUP), val, swap, offheap);
        }
        finally {
            cache0.removeAll(new HashSet<>(primaryKeys));
            cache0.removeAll(new HashSet<>(backupKeys));
        }
    }

    /**
     * @param nodeIdx Node index.
     * @throws Exception If failed.
     */
    private void checkLocalEntriesAffinityFilter(int nodeIdx) throws Exception {
        IgniteCache<Integer, String> cache0 = jcache(nodeIdx);

        final int PUT_KEYS = 10;

        List<Integer> primaryKeys = null;
        List<Integer> backupKeys = null;
        List<Integer> nearKeys = null;

        try {
            primaryKeys = primaryKeys(cache0, PUT_KEYS, 0);
            backupKeys = backupKeys(cache0, PUT_KEYS, 0);

            for (Integer key : primaryKeys)
                cache0.put(key, String.valueOf(key));
            for (Integer key : backupKeys)
                cache0.put(key, String.valueOf(key));

            nearKeys = cacheMode() == PARTITIONED ? nearKeys(cache0, PUT_KEYS, 0) : Collections.<Integer>emptyList();

            for (Integer key : nearKeys)
                cache0.put(key, String.valueOf(key));

            log.info("Keys [near=" + nearKeys + ", primary=" + primaryKeys + ", backup=" + backupKeys + ']');

            if (hasNearCache()) {
                checkLocalEntries(cache0.localEntries(), nearKeys, primaryKeys, backupKeys);
                checkLocalEntries(cache0.localEntries(ALL), nearKeys, primaryKeys, backupKeys);
                checkLocalEntries(cache0.localEntries(NEAR), nearKeys);
                checkLocalEntries(cache0.localEntries(PRIMARY, BACKUP, NEAR), nearKeys, primaryKeys, backupKeys);
                checkLocalEntries(cache0.localEntries(NEAR, PRIMARY), nearKeys, primaryKeys);
                checkLocalEntries(cache0.localEntries(NEAR, BACKUP), nearKeys, backupKeys);
            }
            else {
                checkLocalEntries(cache0.localEntries(), primaryKeys, backupKeys);
                checkLocalEntries(cache0.localEntries(ALL), primaryKeys, backupKeys);
                checkLocalEntries(cache0.localEntries(NEAR));
                checkLocalEntries(cache0.localEntries(NEAR, PRIMARY), primaryKeys);
                checkLocalEntries(cache0.localEntries(NEAR, BACKUP), backupKeys);
                checkLocalEntries(cache0.localEntries(PRIMARY, BACKUP, NEAR), primaryKeys, backupKeys);
            }

            checkLocalEntries(cache0.localEntries(PRIMARY), primaryKeys);
            checkLocalEntries(cache0.localEntries(BACKUP), backupKeys);
            checkLocalEntries(cache0.localEntries(PRIMARY, BACKUP), primaryKeys, backupKeys);
        }
        finally {
            if (primaryKeys != null)
                cache0.removeAll(new HashSet<>(primaryKeys));

            if (backupKeys != null)
                cache0.removeAll(new HashSet<>(backupKeys));

            if (nearKeys != null)
                cache0.removeAll(new HashSet<>(nearKeys));
        }
    }

    /**
     * @param entries Entries.
     * @param exp Expected entries.
     */
    private void checkLocalEntries(Iterable<Cache.Entry<Integer, String>> entries, Collection<Integer>... exp) {
        checkLocalEntries(entries, "", exp);
    }

    /**
     * @param entries Entries.
     * @param expVal Expected value.
     * @param exp Expected keys.
     */
    private void checkLocalEntries(Iterable<Cache.Entry<Integer, String>> entries,
        String expVal,
        Collection<Integer>... exp) {
        Set<Integer> allExp = new HashSet<>();

        for (Collection<Integer> col : exp)
            assertTrue(allExp.addAll(col));

        for (Cache.Entry<Integer, String> e : entries) {
            assertNotNull(e.getKey());
            assertNotNull(e.getValue());
            assertEquals(expVal + e.getKey(), e.getValue());

            assertTrue("Unexpected entry: " + e, allExp.remove(e.getKey()));
        }

        assertTrue("Expected entries not found: " + allExp, allExp.isEmpty());
    }
}