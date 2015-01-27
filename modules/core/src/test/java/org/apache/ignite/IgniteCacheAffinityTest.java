package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.consistenthash.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests for {@link org.apache.ignite.internal.processors.affinity.GridAffinityProcessor.CacheAffinityProxy}.
 */
public class IgniteCacheAffinityTest extends IgniteCacheAbstractTest {
    /** Initial grid count. */
    private int GRID_COUNT = 3;

    /** Cache name */
    private final String CACHE1 = "ConsistentHash";

    /** Cache name */
    private final String CACHE2 = "Fair";

    /** Cache name */
    private final String CACHE3 = "Rendezvous";

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
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected CacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /**
     * Throws Exception if failed.
     */
    public void testAffinity() throws Exception {
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
        checkAffinity(grid(0).affinity(null), grid(1).cache(null).affinity());
        checkAffinity(grid(0).affinity(CACHE1), grid(1).cache(CACHE1).affinity());
        checkAffinity(grid(0).affinity(CACHE2), grid(1).cache(CACHE2).affinity());
        checkAffinity(grid(0).affinity(CACHE3), grid(1).cache(CACHE3).affinity());
    }

    /**
     * @param testAff Cache affinity to test.
     * @param aff Cache affinity.
     */
    private void checkAffinity(CacheAffinity testAff, CacheAffinity aff) {
        checkAffinityKey(testAff, aff);
        checkPartitions(testAff, aff);
        checkIsBackupOrPrimary(testAff, aff);
        checkMapKeyToNode(testAff, aff);
        checkMapKeysToNodes(testAff, aff);
        checkMapPartitionsToNodes(testAff, aff);
    }

    /**
     * Check affinityKey method.
     */
    private void checkAffinityKey(CacheAffinity testAff,CacheAffinity aff) {
        for (int i = 0; i < 10000; i++)
            assertEquals(testAff.affinityKey(i), aff.affinityKey(i));
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
     * Check isBackup, isPrimary and isPrimaryOrBackup methods.
     */
    private void checkIsBackupOrPrimary(CacheAffinity testAff, CacheAffinity aff) {
        for (int i = 0; i < 10000; i++)
            for (ClusterNode n : nodes()) {
                assertEquals(testAff.isBackup(n, i), aff.isBackup(n, i));

                assertEquals(testAff.isPrimary(n, i), aff.isPrimary(n, i));

                assertEquals(testAff.isPrimaryOrBackup(n, i), aff.isPrimaryOrBackup(n, i));
            }
    }

    /**
     * Check mapKeyToNode, mapKeyToPrimaryAndBackups, mapPartitionToPrimaryAndBackups and mapPartitionToNode methods.
     */
    private void checkMapKeyToNode(CacheAffinity testAff, CacheAffinity aff) {
        for (int i = 0; i < 10000; i++) {
            assertEquals(testAff.mapKeyToNode(i).id(), aff.mapKeyToNode(i).id());

            checkEqualCollection(testAff.mapKeyToPrimaryAndBackups(i), aff.mapKeyToPrimaryAndBackups(i));
        }

        assertEquals(aff.partitions(), testAff.partitions());

        for (int part = 0; part < aff.partitions(); ++part) {
            assertEquals(testAff.mapPartitionToNode(part).id(), aff.mapPartitionToNode(part).id());

            checkEqualCollection(testAff.mapPartitionToPrimaryAndBackups(part),
                aff.mapPartitionToPrimaryAndBackups(part));
        }
    }

    /**
     * Check mapKeysToNodes methods.
     */
    private void checkMapKeysToNodes(CacheAffinity testAff, CacheAffinity aff) {
        List<Integer> keys = new ArrayList<>(10000);

        for (int i = 0; i < 10000; ++i)
            keys.add(i);

        checkEqualMaps(testAff.mapKeysToNodes(keys), aff.mapKeysToNodes(keys));
    }

    /**
     * Check mapPartitionsToNodes methods.
     */
    private void checkMapPartitionsToNodes(CacheAffinity testAff, CacheAffinity aff) {
        List<Integer> parts = new ArrayList<>(aff.partitions());

        for (int i = 0; i < aff.partitions(); ++i)
            parts.add(i);

        checkEqualPartitionMaps(testAff.mapPartitionsToNodes(parts), aff.mapPartitionsToNodes(parts));
    }

    /**
     * Check equal arrays.
     */
    private static void checkEqualIntArray(int[] arr1, int[] arr2) {
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
    private static void checkEqualCollection(Collection<ClusterNode> col1, Collection<ClusterNode> col2) {
        assertEquals(col1.size(), col2.size());

        for (ClusterNode node : col1)
            assertTrue(col2.contains(node));
    }

    /**
     * Check equal maps.
     */
    private static void checkEqualMaps(Map<ClusterNode, Collection> map1, Map<ClusterNode, Collection> map2) {
        assertEquals(map1.size(), map2.size());

        for (ClusterNode node : map1.keySet()) {
            assertTrue(map2.containsKey(node));

            assertEquals(map1.get(node).size(), map2.get(node).size());
        }
    }

    /**
     * Check equal maps.
     */
    private static void checkEqualPartitionMaps(Map<Integer, ClusterNode> map1, Map<Integer, ClusterNode> map2) {
        assertEquals(map1.size(), map2.size());

        for (Integer i : map1.keySet()) {
            assertTrue(map2.containsKey(i));

            assertEquals(map1.get(i), map2.get(i));
        }
    }

    /**
     * @return Cluster nodes.
     */
    private Collection<ClusterNode> nodes() {
        return grid(0).nodes();
    }
}
