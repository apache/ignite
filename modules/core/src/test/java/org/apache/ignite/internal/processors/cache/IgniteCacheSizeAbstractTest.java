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
        for (int i = 0; i < gridCount(); i++) {
            IgniteCache<Integer, String> cache = jcache(i);

            assertEquals(0, cache.localSize());

            assertEquals(0, cache.size());

            for (CachePeekMode peekMode : CachePeekMode.values()) {
                assertEquals(0, cache.localSize(peekMode));

                assertEquals(0, cache.size(peekMode));
            }
        }

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
        }
        else {
            checkSizeAffinityFilter(0);

            checkSizeAffinityFilter(1);
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
        }
        finally {
            if (keys != null)
                cache0.removeAll(new HashSet<>(keys));
        }
    }

    /**
     * @param nodeIdx Node index.
     * @throws Exception If failed.
     */
    private void checkSizeStorageFilter(int nodeIdx) throws Exception {

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
