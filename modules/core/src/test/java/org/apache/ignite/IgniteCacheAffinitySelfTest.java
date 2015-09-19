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

package org.apache.ignite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for {@link GridAffinityProcessor.CacheAffinityProxy}.
 */
public class IgniteCacheAffinitySelfTest extends IgniteCacheAbstractTest {
    /** Initial grid count. */
    private int GRID_CNT = 3;

    /** Cache name */
    private final String CACHE1 = "Fair";

    /** Cache name */
    private final String CACHE2 = "Rendezvous";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache0 = cacheConfiguration(null);

        CacheConfiguration cache1 = cacheConfiguration(null);
        cache1.setName(CACHE1);
        cache1.setAffinity(new FairAffinityFunction());

        CacheConfiguration cache2 = cacheConfiguration(null);
        cache2.setName(CACHE2);
        cache2.setAffinity(new RendezvousAffinityFunction());

        if (gridName.contains("0"))
            cfg.setCacheConfiguration(cache0);
        else
            cfg.setCacheConfiguration(cache0, cache1, cache2);

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
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        fail("Enable when https://issues.apache.org/jira/browse/IGNITE-647 is fixed.");
    }

    /**
     * @throws Exception if failed.
     */
    public void testAffinity() throws Exception {
        fail("Enable when https://issues.apache.org/jira/browse/IGNITE-647 is fixed.");

        checkAffinity();

        stopGrid(gridCount() - 1);

        startGrid(gridCount() - 1);
        startGrid(gridCount());

        GRID_CNT += 1;

        checkAffinity();
    }

    /**
     * Check CacheAffinityProxy methods.
     */
    private void checkAffinity() {
        checkAffinity(grid(0).affinity(null), internalCache(1, null).affinity());
        checkAffinity(grid(0).affinity(CACHE1), internalCache(1, CACHE1).affinity());
        checkAffinity(grid(0).affinity(CACHE1), internalCache(1, CACHE1).affinity());
        checkAffinity(grid(0).affinity(CACHE2), internalCache(1, CACHE2).affinity());
    }

    /**
     * @param testAff Cache affinity to test.
     * @param aff Cache affinity.
     */
    private void checkAffinity(Affinity testAff, Affinity aff) {
        checkAffinityKey(testAff, aff);
        checkPartitions(testAff, aff);
        checkIsBackupOrPrimary(testAff, aff);
        checkMapKeyToNode(testAff, aff);
        checkMapKeysToNodes(testAff, aff);
        checkMapPartitionToNode(testAff, aff);
        checkMapPartitionsToNodes(testAff, aff);
    }

    /**
     * Check affinityKey method.
     *
     * @param testAff Affinity1.
     * @param aff Affinity2.
     */
    private void checkAffinityKey(Affinity testAff, Affinity aff) {
        for (int i = 0; i < 10000; i++)
            assertEquals(testAff.affinityKey(i), aff.affinityKey(i));
    }

    /**
     * Check allPartitions, backupPartitions and primaryPartitions methods.
     *
     * @param testAff Affinity1.
     * @param aff Affinity2.
     */
    private void checkPartitions(Affinity testAff, Affinity aff) {
        for (ClusterNode n : nodes()) {
            checkEqualIntArray(testAff.allPartitions(n), aff.allPartitions(n));

            checkEqualIntArray(testAff.backupPartitions(n), aff.backupPartitions(n));

            checkEqualIntArray(testAff.primaryPartitions(n), aff.primaryPartitions(n));
        }
    }

    /**
     * Check isBackup, isPrimary and isPrimaryOrBackup methods.
     *
     * @param testAff Affinity1.
     * @param aff Affinity2.
     */
    private void checkIsBackupOrPrimary(Affinity testAff, Affinity aff) {
        for (int i = 0; i < 10000; i++)
            for (ClusterNode n : nodes()) {
                assertEquals(testAff.isBackup(n, i), aff.isBackup(n, i));

                assertEquals(testAff.isPrimary(n, i), aff.isPrimary(n, i));

                assertEquals(testAff.isPrimaryOrBackup(n, i), aff.isPrimaryOrBackup(n, i));
            }
    }

    /**
     * Check mapKeyToNode, mapKeyToPrimaryAndBackups methods.
     *
     * @param testAff Affinity1.
     * @param aff Affinity2.
     */
    private void checkMapKeyToNode(Affinity testAff, Affinity aff) {
        for (int i = 0; i < 10000; i++) {
            assertEquals(testAff.mapKeyToNode(i).id(), aff.mapKeyToNode(i).id());

            checkEqualCollection(testAff.mapKeyToPrimaryAndBackups(i), aff.mapKeyToPrimaryAndBackups(i));
        }
    }

    /**
     * Check mapPartitionToPrimaryAndBackups and mapPartitionToNode methods.
     *
     * @param testAff Affinity1.
     * @param aff Affinity2.
     */
    private void checkMapPartitionToNode(Affinity testAff, Affinity aff) {
        assertEquals(aff.partitions(), testAff.partitions());

        for (int part = 0; part < aff.partitions(); ++part) {
            assertEquals(testAff.mapPartitionToNode(part).id(), aff.mapPartitionToNode(part).id());

            checkEqualCollection(testAff.mapPartitionToPrimaryAndBackups(part),
                aff.mapPartitionToPrimaryAndBackups(part));
        }
    }

    /**
     * Check mapKeysToNodes methods.
     *
     * @param testAff Affinity1.
     * @param aff Affinity2.
     */
    private void checkMapKeysToNodes(Affinity testAff, Affinity aff) {
        List<Integer> keys = new ArrayList<>(10000);

        for (int i = 0; i < 10000; ++i)
            keys.add(i);

        checkEqualMaps(testAff.mapKeysToNodes(keys), aff.mapKeysToNodes(keys));
    }

    /**
     * Check mapPartitionsToNodes methods.
     *
     * @param testAff Affinity1.
     * @param aff Affinity2.
     */
    private void checkMapPartitionsToNodes(Affinity testAff, Affinity aff) {
        List<Integer> parts = new ArrayList<>(aff.partitions());

        for (int i = 0; i < aff.partitions(); ++i)
            parts.add(i);

        checkEqualPartitionMaps(testAff.mapPartitionsToNodes(parts), aff.mapPartitionsToNodes(parts));
    }

    /**
     * Check equal arrays.
     *
     * @param arr1 Array 1.
     * @param arr2 Array 2.
     */
    private static void checkEqualIntArray(int[] arr1, int[] arr2) {
        assertEquals(arr1.length, arr2.length);

        Collection<Integer> col1 = new HashSet<>();

        for (int anArr1 : arr1)
            col1.add(anArr1);

        for (int anArr2 : arr2) {
            assertTrue(col1.contains(anArr2));

            col1.remove(anArr2);
        }

        assertEquals(0, col1.size());
    }

    /**
     * Check equal collections.
     *
     * @param col1 Collection 1.
     * @param col2 Collection 2.
     */
    private static void checkEqualCollection(Collection<ClusterNode> col1, Collection<ClusterNode> col2) {
        assertEquals(col1.size(), col2.size());

        for (ClusterNode node : col1)
            assertTrue(col2.contains(node));
    }

    /**
     * Check equal maps.
     *
     * @param map1 Map1.
     * @param map2 Map2.
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
     *
     * @param map1 Map1.
     * @param map2 Map2.
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
        return grid(0).cluster().nodes();
    }
}