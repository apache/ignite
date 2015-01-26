package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;

import javax.cache.*;
import java.util.*;

/**
 * Tests affinity cache.
 */
public class IgniteCacheAffinityTest extends IgniteCacheAbstractTest {
    private final int GRID_COUNT = 4;
    private final String CACHE1 = "cache1";
    private final String CACHE2 = "cache2";


    @Override
    protected int gridCount() {
        return GRID_COUNT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache0 = cacheConfiguration(null);

        CacheConfiguration cache1 = cacheConfiguration(null);
        cache1.setName(CACHE1);

        CacheConfiguration cache2 = cacheConfiguration(null);
        cache2.setName(CACHE2);


        if (gridName.contains("0")) {
            cfg.setCacheConfiguration(cache0);
        }
        else if (gridName.contains("1")) {
            cfg.setCacheConfiguration(cache0, cache1);
        }
        else if (gridName.contains("2")) {
            cfg.setCacheConfiguration();
        }
        else {
            cfg.setCacheConfiguration(cache0, cache1, cache2);
        }
        return cfg;
    }

    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    @Override protected CacheDistributionMode distributionMode() {
        return CacheDistributionMode.PARTITIONED_ONLY;
    }

    public void testAffinity() throws Exception {

        GridCache<String, Integer> cache0 =  grid(0).cache(null);
        GridCache<String, Integer> cache1 =  grid(1).cache(CACHE1);
        GridCache<String, Integer> cache2 =  grid(3).cache(CACHE2);

        cache0.affinity();

        for (int i = 0; i < 10; ++i)
            cache0.put(Integer.toString(i), i);

        for (int i = 10; i < 20; ++i)
            cache1.put(Integer.toString(i), i);

        for (int i = 20; i < 30; ++i)
            cache2.put(Integer.toString(i), i);

        checkAffinity(gridCount());

        startGrid(4);

        startGrid(5);

        checkAffinity(6);

    }

    private void checkAffinity(int n) {
        for (int i = 0; i < n; ++i) {
            if (grid(i).cachex(null) != null)
                checkAffinity(i, grid(i).<String, Integer>cache(null));

            if (grid(i).cachex(CACHE1) != null)
                checkAffinity(i, grid(i).<String, Integer>cache(null));

            if (grid(i).cachex(CACHE2) != null)
                checkAffinity(i, grid(i).<String, Integer>cache(null));
        }
    }

    private void checkAffinity(int idx, GridCache<String, Integer> cache) {
        for (int i = 0; i < gridCount(); ++i)
            checkGridAffinity(grid(i), idx, cache);
    }

    private void checkGridAffinity(Ignite ignite, int idx,  GridCache<String, Integer> cache) {
        IgniteCache<String, Integer> jcache = grid(idx).jcache(cache.name());
        checkAffinityKey(ignite, jcache, cache.affinity());
        checkPartitions(ignite, cache.name(), cache.affinity());
        checkIsBackupOrPrimary(ignite, jcache, cache.affinity());
        checkMapKeyToNode(ignite, jcache, cache.affinity());
    }

    private void checkMapKeyToNode(Ignite ignite, IgniteCache<String, Integer> jcache, CacheAffinity<String> aff) {
        CacheAffinity<String> igniteAff = ignite.affinity(jcache.getName());

        Iterator<Cache.Entry<String, Integer>> iter = jcache.iterator();



        while (iter.hasNext()) {
            Cache.Entry<String, Integer> entry = iter.next();
            UUID node1 = igniteAff.mapKeyToNode(entry.getKey()).id();
            UUID node2 = aff.mapKeyToNode(entry.getKey()).id();
            assertEquals(node1, node2);

            Collection<ClusterNode> nodes1 = igniteAff.mapKeyToPrimaryAndBackups(entry.getKey());
            Collection<ClusterNode> nodes2 = aff.mapKeyToPrimaryAndBackups(entry.getKey());
            checkEqualCollection(nodes1, nodes2);
        }
    }




    private void checkAffinityKey(Ignite ignite, IgniteCache<String, Integer> jcache, CacheAffinity<String> aff) {
        CacheAffinity<String> igniteAff = ignite.affinity(jcache.getName());

        Iterator<Cache.Entry<String, Integer>> iter = jcache.iterator();
        while (iter.hasNext()) {
            Cache.Entry<String, Integer> entry = iter.next();
            assertEquals(igniteAff.affinityKey(entry.getKey()), (aff.affinityKey(entry.getKey())));
        }
    }

    private void checkIsBackupOrPrimary(Ignite ignite, IgniteCache<String, Integer> jcache, CacheAffinity<String> aff) {
        CacheAffinity<String> igniteAff = ignite.affinity(jcache.getName());

        Iterator<Cache.Entry<String, Integer>> iter = jcache.iterator();
        while (iter.hasNext()) {
            Cache.Entry<String, Integer> entry = iter.next();
            for (ClusterNode n : ignite.cluster().nodes()) {
                assertEquals(igniteAff.isBackup(n, entry.getKey()), aff.isBackup(n, entry.getKey()));
                assertEquals(igniteAff.isPrimary(n, entry.getKey()), aff.isPrimary(n, entry.getKey()));
                assertEquals(igniteAff.isPrimaryOrBackup(n, entry.getKey()), aff.isPrimaryOrBackup(n, entry.getKey()));
            }
        }

    }

    private void checkPartitions(Ignite ignite, String cacheName, CacheAffinity<String> aff) {
        for (ClusterNode n : ignite.cluster().nodes()) {
            checkEqualIntArray(ignite.affinity(cacheName).allPartitions(n), aff.allPartitions(n));
            checkEqualIntArray(ignite.affinity(cacheName).backupPartitions(n), aff.backupPartitions(n));
            checkEqualIntArray(ignite.affinity(cacheName).primaryPartitions(n), aff.primaryPartitions(n));
        }
    }

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

    private void checkEqualCollection(Collection<ClusterNode> col1, Collection<ClusterNode> col2) {
        Collection<ClusterNode> colCopy1 = new HashSet<>(col1);
        for (ClusterNode node : col2) {
            assertTrue(colCopy1.contains(node));
            colCopy1.remove(node);
        }

        assertEquals(0, colCopy1.size());
    }
}
