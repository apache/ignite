/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests for partitioned cache.
 */
public class GridCachePartitionedFullApiSelfTest extends GridCacheAbstractFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setEvictNearSynchronized(false);
        cfg.setEvictSynchronized(false);

        cfg.setAtomicityMode(atomicityMode());
        cfg.setSwapEnabled(true);
        cfg.setTxSerializableEnabled(true);

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
                Grid g = grid(i);

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
                Grid g = grid(i);

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
                Grid g = grid(i);

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
