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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.consistenthash.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;

import java.util.*;

import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Tests for partitioned cache.
 */
public class GridCachePartitionedFullApiSelfTest extends GridCacheAbstractFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.getTransactionsConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setEvictNearSynchronized(false);
        cfg.setEvictSynchronized(false);

        cfg.setAtomicityMode(atomicityMode());
        cfg.setSwapEnabled(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionEntrySetToString() throws Exception {
        GridCacheAdapter<String, Integer> cache = ((GridKernal)grid(0)).internalCache();

        for (int i = 0; i < 100; i++) {
            String key = String.valueOf(i);

            cache.put(key, i);
        }

        GridCacheConsistentHashAffinityFunction aff = (GridCacheConsistentHashAffinityFunction)cache.configuration().getAffinity();

        for (int i = 0 ; i < aff.getPartitions(); i++)
            String.valueOf(cache.entrySet(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionEntrySetIterator() throws Exception {
        GridCacheAdapter<String, Integer> cache = ((GridKernal)grid(0)).internalCache();

        Map<Integer, Collection<String>> partMap = new HashMap<>();

        for (int i = 0; i < 1000; i++) {
            String key = String.valueOf(i);

            int part = cache.affinity().partition(key);

            cache.put(key, i);

            Collection<String> keys = partMap.get(part);

            if (keys == null) {
                keys = new LinkedList<>();

                partMap.put(part, keys);
            }

            keys.add(key);
        }

        for (Map.Entry<Integer, Collection<String>> entry : partMap.entrySet()) {
            int part = entry.getKey();
            Collection<String> vals = entry.getValue();

            String key = F.first(vals);

            for (int i = 0; i < gridCount(); i++) {
                Ignite g = grid(i);

                // This node has the partition.
                GridCache<String, Integer> nodeCache = g.cache(null);

                if (offheapTiered(nodeCache))
                    continue;

                Set<GridCacheEntry<String, Integer>> partEntrySet = nodeCache.entrySet(part);

                if (nodeCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key)) {
                    Collection<String> cp = new LinkedList<>(vals);

                    for (GridCacheEntry<String, Integer> e : partEntrySet) {
                        String eKey = e.getKey();

                        assertTrue("Got unexpected key:" + eKey, cp.contains(eKey));

                        // Check contains.
                        assertTrue(partEntrySet.contains(nodeCache.entry(eKey)));
                        assertTrue(partEntrySet.contains(e));

                        cp.remove(eKey);
                    }

                    assertTrue("Keys are not in partition entry set: " + cp, cp.isEmpty());
                }
                else
                    assert partEntrySet == null || partEntrySet.isEmpty();
            }
        }

        Collection<String> deleted = new LinkedList<>();

        // Check remove.
        for (Map.Entry<Integer, Collection<String>> entry : partMap.entrySet()) {
            int part = entry.getKey();
            Collection<String> vals = entry.getValue();

            String key = F.first(vals);

            for (int i = 0; i < gridCount(); i++) {
                Ignite g = grid(i);

                // This node has the partition.
                GridCache<String, Integer> nodeCache = g.cache(null);

                if (offheapTiered(nodeCache))
                    continue;

                // First node with this partition will remove first key from partition.
                if (nodeCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key)) {
                    Set<GridCacheEntry<String, Integer>> partEntrySet = nodeCache.entrySet(part);

                    Iterator<GridCacheEntry<String, Integer>> it = partEntrySet.iterator();

                    assertTrue(it.hasNext());

                    GridCacheEntry<String, Integer> next = it.next();

                    deleted.add(next.getKey());

                    it.remove();

                    break; // For.
                }
            }
        }

        for (String delKey : deleted) {
            for (int i = 0; i < gridCount(); i++)
                assertNull(grid(i).cache(null).get(delKey));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionEntrySetRemove() throws Exception {
        GridCache<String, Integer> cache = cache(0);

        Map<Integer, Collection<String>> partMap = new HashMap<>();

        for (int i = 0; i < 1000; i++) {
            String key = String.valueOf(i);

            int part = cache.affinity().partition(key);

            cache.put(key, i);

            Collection<String> keys = partMap.get(part);

            if (keys == null) {
                keys = new LinkedList<>();

                partMap.put(part, keys);
            }

            keys.add(key);
        }

        Collection<String> deleted = new LinkedList<>();

        for (Map.Entry<Integer, Collection<String>> entry : partMap.entrySet()) {
            int part = entry.getKey();
            Collection<String> vals = entry.getValue();

            String key = F.first(vals);

            for (int i = 0; i < gridCount(); i++) {
                Ignite g = grid(i);

                // This node has the partition.
                GridCache<String, Integer> nodeCache = g.cache(null);

                if (offheapTiered(nodeCache))
                    continue;

                Set<GridCacheEntry<String, Integer>> partEntrySet = nodeCache.entrySet(part);

                if (nodeCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key)) {
                    assertTrue(partEntrySet.contains(nodeCache.entry(key)));

                    deleted.add(key);

                    assertTrue(partEntrySet.remove(nodeCache.entry(key)));

                    break; // For.
                }
            }
        }

        for (String delKey : deleted) {
            for (int i = 0; i < gridCount(); i++)
                assertNull(grid(i).cache(null).get(delKey));
        }
    }
}
