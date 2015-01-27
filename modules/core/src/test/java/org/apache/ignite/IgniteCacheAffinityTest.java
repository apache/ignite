package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.consistenthash.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;

import javax.cache.*;
import java.util.*;

/**
 * Tests for {@link org.apache.ignite.internal.processors.affinity.GridAffinityProcessor.CacheAffinityProxy}.
 */
public class IgniteCacheAffinityTest extends IgniteCacheAbstractTest {
    /** Initial grid count. */
    private int GRID_COUNT = 3;

    /** Cache name */
    private final String CACHE1 = "ConsistentCache";

    /** Cache name */
    private final String CACHE2 = "PartitionFairCache";

    /** Cache name */
    private final String CACHE3 = "RendezvousCache";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_COUNT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache0 = cacheConfiguration(null);

        CacheConfiguration cache1 = cacheConfiguration(null);
        cache1.setName(CACHE1);
        cache1.setAffinity(new CacheConsistentHashAffinityFunction());

        CacheConfiguration cache2 = cacheConfiguration(null);
        cache2.setName(CACHE2);
        cache2.setAffinity(new CachePartitionFairAffinity());

        CacheConfiguration cache3 = cacheConfiguration(null);
        cache3.setName(CACHE3);
        cache3.setAffinity(new CacheRendezvousAffinityFunction());

        if (gridName.contains("0"))
            cfg.setCacheConfiguration(cache0);
        else
            cfg.setCacheConfiguration(cache0, cache1, cache2, cache3);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected CacheDistributionMode distributionMode() {
        return CacheDistributionMode.NEAR_PARTITIONED;
    }

    /**
     * Throws Exception if failed.
     */
    public void testAffinity() throws Exception {
        if (cacheMode().equals(CacheMode.LOCAL)) {
            info("Test is not applied for local cache.");

            return;
        }

        GridCache<String, Integer> cache0 = grid(1).cache(null);
        GridCache<String, Integer> cache1 = grid(1).cache(CACHE1);
        GridCache<String, Integer> cache2 = grid(1).cache(CACHE2);
        GridCache<String, Integer> cache3 = grid(1).cache(CACHE3);

        for (int i = 0; i < 10; ++i)
            cache0.put(Integer.toString(i), i);

        for (int i = 10; i < 20; ++i)
            cache1.put(Integer.toString(i), i);

        for (int i = 20; i < 30; ++i)
            cache2.put(Integer.toString(i), i);

        for (int i = 30; i < 40; ++i)
            cache3.put(Integer.toString(i), i);

        checkAffinity();

        stopGrid(gridCount() - 1);

        startGrid(gridCount() - 1);
        startGrid(gridCount());

        GRID_COUNT += 1;

        checkAffinity();
    }

    /**
     * Check CacheAffinityProxy methods.
     */
    private void checkAffinity() {
        checkGridAffinity(grid(0).affinity(null), grid(1).jcache(null), grid(1).cache(null));

        checkGridAffinity(grid(0).affinity(CACHE1), grid(1).jcache(CACHE1), grid(1).cache(CACHE1));

        checkGridAffinity(grid(0).affinity(CACHE2), grid(1).jcache(CACHE2), grid(1).cache(CACHE2));

        checkGridAffinity(grid(0).affinity(CACHE3), grid(1).jcache(CACHE3), grid(1).cache(CACHE3));
    }

    /**
     * @param testAff Cache affinity to test.
     * @param jcache Ignite cache.
     * @param cache Cache.
     */
    private void checkGridAffinity(CacheAffinity testAff, IgniteCache jcache, GridCache cache) {
        checkAffinityKey(testAff, jcache, cache.affinity());

        checkPartitions(testAff, cache.affinity());

        checkIsBackupOrPrimary(testAff, jcache, cache.affinity());

        checkMapKeyToNode(testAff, jcache, cache.affinity());
    }

    /**
     * Check mapKeyToNode, mapKeyToPrimaryAndBackups and mapPartitionToNode methods.
     */
    private void checkMapKeyToNode(CacheAffinity testAff, IgniteCache jcache, CacheAffinity aff) {
        Iterator<Cache.Entry> iter = jcache.iterator();

        while (iter.hasNext()) {
            Cache.Entry entry = iter.next();

            UUID node1 = testAff.mapKeyToNode(entry.getKey()).id();
            UUID node2 = aff.mapKeyToNode(entry.getKey()).id();

            assertEquals(node1, node2);

            Collection<ClusterNode> nodes1 = testAff.mapKeyToPrimaryAndBackups(entry.getKey());
            Collection<ClusterNode> nodes2 = aff.mapKeyToPrimaryAndBackups(entry.getKey());

            checkEqualCollection(nodes1, nodes2);

            int part = aff.partition(entry.getKey());

            assertEquals(testAff.mapPartitionToNode(part).id(), aff.mapPartitionToNode(part).id());
        }
    }

    /**
     * Check affinityKey method.
     */
    private void checkAffinityKey(CacheAffinity testAff, IgniteCache jcache, CacheAffinity aff) {
        Iterator<Cache.Entry> iter = jcache.iterator();

        while (iter.hasNext()) {
            Cache.Entry entry = iter.next();

            assertEquals(testAff.affinityKey(entry.getKey()), (aff.affinityKey(entry.getKey())));
        }
    }

    /**
     * Check isBackup, isPrimary and isPrimaryOrBackup methods.
     */
    private void checkIsBackupOrPrimary(CacheAffinity testAff, IgniteCache jcache, CacheAffinity aff) {

        Iterator<Cache.Entry> iter = jcache.iterator();

        while (iter.hasNext()) {
            Cache.Entry entry = iter.next();

            for (ClusterNode n : nodes()) {
                assertEquals(testAff.isBackup(n, entry.getKey()), aff.isBackup(n, entry.getKey()));

                assertEquals(testAff.isPrimary(n, entry.getKey()), aff.isPrimary(n, entry.getKey()));

                assertEquals(testAff.isPrimaryOrBackup(n, entry.getKey()), aff.isPrimaryOrBackup(n, entry.getKey()));
            }
        }
    }

    /**
     * Check allPartitions, backupPartitions and primaryPartitions methods.
     */
    private void checkPartitions(CacheAffinity testAff, CacheAffinity aff) {
        for (ClusterNode n : nodes()) {
            checkEqualIntArray(testAff.allPartitions(n), aff.allPartitions(n));

            checkEqualIntArray(testAff.backupPartitions(n), aff.backupPartitions(n));

            checkEqualIntArray(testAff.primaryPartitions(n), aff.primaryPartitions(n));
        }
    }

    /**
     * Check equal arrays.
     */
    private void checkEqualIntArray(int[] arr1, int[] arr2) {
        assertEquals(arr1.length, arr2.length);

        Collection<Integer> col1 = new HashSet<>();

        for (int i = 0; i < arr1.length; ++i)
            col1.add(arr1[i]);

        for (int i = 0; i < arr2.length; ++i) {
            assertTrue(col1.contains(arr2[i]));

            col1.remove(arr2[i]);
        }

        assertEquals(0, col1.size());
    }

    /**
     * Check equal collections.
     */
    private void checkEqualCollection(Collection<ClusterNode> col1, Collection<ClusterNode> col2) {
        Collection<ClusterNode> colCopy1 = new HashSet<>(col1);

        for (ClusterNode node : col2) {
            assertTrue(colCopy1.contains(node));
            colCopy1.remove(node);
        }

        assertEquals(0, colCopy1.size());
    }

    /**
     * @return Cluster nodes.
     */
    private Collection<ClusterNode> nodes() {
        Set<ClusterNode> nodes = new HashSet<>();

        for (int i = 0; i < gridCount(); ++i)
            nodes.addAll(grid(i).nodes());

        return nodes;
    }
}
