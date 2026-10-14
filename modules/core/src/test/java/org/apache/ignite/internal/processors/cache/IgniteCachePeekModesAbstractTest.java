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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.ALL;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;
import static org.apache.ignite.cache.CachePeekMode.NEAR;
import static org.apache.ignite.cache.CachePeekMode.OFFHEAP;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;

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
    private static final int HEAP_ENTRIES = 30;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg;
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
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setBackups(1);

        if (hasNearCache())
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(HEAP_ENTRIES);

        ccfg.setEvictionPolicy(plc);
        ccfg.setOnheapCacheEnabled(true);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPeek() throws Exception {
        checkAffinityPeek(0);

        checkAffinityPeek(1);

        checkStorage(0);

        checkStorage(1);
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

            Affinity<Integer> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

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
        if (true) // TODO GG-11148.
            return;

        IgniteCache<Integer, String> cache0 = jcache(nodeIdx);

        List<Integer> keys = primaryKeys(cache0, 100, 10_000);

        try {
            final String val = "test_value";

            for (Integer key : keys)
                cache0.put(key, val);

            Ignite ignite = ignite(nodeIdx);

            GridCacheAdapter<Integer, String> internalCache =
                ((IgniteKernal)ignite).context().cache().internalCache(DEFAULT_CACHE_NAME);

            CacheObjectContext coctx = internalCache.context().cacheObjectContext();

            Set<Integer> swapKeys = new HashSet<>();

// TODO GG-11148.
//            SwapSpaceSpi swap = ignite.configuration().getSwapSpaceSpi();
//
//            IgniteSpiCloseableIterator<KeyCacheObject> it = swap.keyIterator(SPACE_NAME, null);

            IgniteSpiCloseableIterator<KeyCacheObject> it = new GridEmptyCloseableIterator<>();

            assertNotNull(it);

            while (it.hasNext()) {
                KeyCacheObject next = it.next();

                assertTrue(swapKeys.add((Integer)next.value(coctx, false)));
            }

            assertFalse(swapKeys.isEmpty());

            assertTrue(swapKeys.size() + HEAP_ENTRIES < 100);

            Set<Integer> offheapKeys = new HashSet<>();

// TODO GG-11148.
            Iterator<Map.Entry<Integer, String>> offheapIt = Collections.EMPTY_MAP.entrySet().iterator();

//            if (internalCache.context().isNear())
//                offheapIt = internalCache.context().near().dht().context().swap().lazyOffHeapIterator(false);
//            else
//                offheapIt = internalCache.context().swap().lazyOffHeapIterator(false);

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
                assertEquals(val, cache0.localPeek(key));
                assertEquals(val, cache0.localPeek(key, PRIMARY));
                assertEquals(val, cache0.localPeek(key, ONHEAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP, OFFHEAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, ONHEAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, ONHEAP, OFFHEAP));

                assertNull(cache0.localPeek(key, BACKUP));
                assertNull(cache0.localPeek(key, NEAR));

                assertNull(cache0.localPeek(key, ONHEAP));
                assertNull(cache0.localPeek(key, OFFHEAP));
            }

            for (Integer key : offheapKeys) {
                assertEquals(val, cache0.localPeek(key, OFFHEAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP, OFFHEAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP, OFFHEAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, OFFHEAP));

                assertNull(cache0.localPeek(key, OFFHEAP, BACKUP));
                assertNull(cache0.localPeek(key, OFFHEAP, NEAR));

                assertNull(cache0.localPeek(key, ONHEAP));
                assertNull(cache0.localPeek(key));
            }

            for (Integer key : heapKeys) {
                assertEquals(val, cache0.localPeek(key, ONHEAP));
                assertEquals(val, cache0.localPeek(key, ONHEAP));
                assertEquals(val, cache0.localPeek(key, OFFHEAP, ONHEAP));
                assertEquals(val, cache0.localPeek(key, PRIMARY, ONHEAP));

                assertNull(cache0.localPeek(key, ONHEAP, BACKUP));
                assertNull(cache0.localPeek(key, ONHEAP, NEAR));

                assertNull(cache0.localPeek(key));
                assertNull(cache0.localPeek(key, OFFHEAP));
            }
        }
        finally {
            cache0.removeAll(new HashSet<>(keys));
        }
    }
}
