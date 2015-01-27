package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;

import javax.cache.*;
import java.util.*;

/**
 * Tests for {@link org.apache.ignite.internal.processors.affinity.GridAffinityProcessor.CacheAffinityProxy}.
 */
public abstract class IgniteCacheAffinityAbstractTest extends IgniteCacheAbstractTest {
    /** Initial grid count. */
    private int GRID_COUNT = 3;

    /** Cache name */
    private final String CACHE1 = "cache1";

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

        if (gridName.contains("0")) {
            cfg.setCacheConfiguration();
        }
        else if (gridName.contains("1")) {
            cfg.setCacheConfiguration(cache0);
        }
        else {
            cfg.setCacheConfiguration(cache0, cache1);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
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
        GridCache<String, Integer> cache1 = grid(2).cache(CACHE1);

        for (int i = 0; i < 10; ++i)
            cache0.put(Integer.toString(i), i);

        for (int i = 10; i < 20; ++i)
            cache1.put(Integer.toString(i), i);

        checkAffinity();

        startGrid(gridCount());
        startGrid(gridCount() + 1);

        GRID_COUNT += 2;

        checkAffinity();
    }

    /**
     * Check CacheAffinityProxy methods.
     */
    private void checkAffinity() {
        for (int i = 0; i < gridCount(); ++i) {
            if (grid(i).cachex(null) != null)
                checkAffinity(grid(i).<String, Integer>jcache(null), grid(i).<String, Integer>cache(null));

            if (grid(i).cachex(CACHE1) != null)
                checkAffinity(grid(i).<String, Integer>jcache(CACHE1), grid(i).<String, Integer>cache(CACHE1));
        }
    }

    /**
     * @param jcache Jcache to iterate over.
     * @param cache Cache to check.
     */
    private void checkAffinity(IgniteCache<String, Integer> jcache, GridCache<String, Integer> cache) {
        for (int i = 0; i < gridCount(); ++i)
            checkGridAffinity(grid(i).<String>affinity(cache.name()), jcache, cache);
    }

    /**
     * @param testAff Cache affinity to test.
     * @param jcache Ignite cache.
     * @param cache Cache.
     */
    private void checkGridAffinity(CacheAffinity<String> testAff, IgniteCache<String, Integer> jcache,
        GridCache<String, Integer> cache) {
        checkAffinityKey(testAff, jcache, cache.affinity());

        checkPartitions(testAff, cache.affinity());

        checkIsBackupOrPrimary(testAff, jcache, cache.affinity());

        checkMapKeyToNode(testAff, jcache, cache.affinity());
    }

    /**
     * Check mapKeyToNode, mapKeyToPrimaryAndBackups and mapPartitionToNode methods.
     */
    private void checkMapKeyToNode(CacheAffinity<String> testAff,
        IgniteCache<String, Integer> jcache, CacheAffinity<String> aff) {
        Iterator<Cache.Entry<String, Integer>> iter = jcache.iterator();

        while (iter.hasNext()) {
            Cache.Entry<String, Integer> entry = iter.next();

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
    private void checkAffinityKey(CacheAffinity<String> testAff,
        IgniteCache<String, Integer> jcache, CacheAffinity<String> aff) {
        Iterator<Cache.Entry<String, Integer>> iter = jcache.iterator();

        while (iter.hasNext()) {
            Cache.Entry<String, Integer> entry = iter.next();

            assertEquals(testAff.affinityKey(entry.getKey()), (aff.affinityKey(entry.getKey())));
        }
    }

    /**
     * Check isBackup, isPrimary and isPrimaryOrBackup methods.
     */
    private void checkIsBackupOrPrimary(CacheAffinity<String> testAff, IgniteCache<String, Integer> jcache,
        CacheAffinity<String> aff) {

        Iterator<Cache.Entry<String, Integer>> iter = jcache.iterator();

        while (iter.hasNext()) {
            Cache.Entry<String, Integer> entry = iter.next();

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
    private void checkPartitions(CacheAffinity<String> testAff, CacheAffinity<String> aff) {
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
    Collection<ClusterNode> nodes() {
        Set<ClusterNode> nodes = new HashSet<>();

        for (int i = 0; i < gridCount(); ++i)
            nodes.addAll(grid(i).nodes());

        return nodes;
    }
}
