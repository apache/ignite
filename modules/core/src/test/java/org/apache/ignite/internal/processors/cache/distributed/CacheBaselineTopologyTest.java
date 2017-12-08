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
 *
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheBaselineTopologyTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private boolean client;

    /** */
    private static final int NODE_COUNT = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        GridTestUtils.deleteDbFiles();

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100 * 1024 * 1024)
                    .setInitialSize(100 * 1024 * 1024)
            )
            .setWalMode(WALMode.LOG_ONLY)
        );

        if (client)
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChangesWithFixedBaseline() throws Exception {
        startGrids(NODE_COUNT);

        IgniteEx ignite = grid(0);

        ignite.active(true);

        awaitPartitionMapExchange();

        Map<ClusterNode, Ignite> nodes = new HashMap<>();

        for (int i = 0; i < NODE_COUNT; i++) {
            Ignite ig = grid(i);

            nodes.put(ig.cluster().localNode(), ig);
        }

        IgniteCache<Integer, Integer> cache =
            ignite.createCache(
                new CacheConfiguration<Integer, Integer>()
                    .setName(CACHE_NAME)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
                    .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
            );

        int key = -1;

        for (int k = 0; k < 100_000; k++) {
            if (!ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(k).contains(ignite.localNode())) {
                key = k;
                break;
            }
        }

        assert key >= 0;

        int part = ignite.affinity(CACHE_NAME).partition(key);

        Collection<ClusterNode> initialMapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping.size() == 2 : initialMapping;

        ignite.cluster().setBaselineTopology(baselineNodes(nodes.keySet()));

        awaitPartitionMapExchange();

        cache.put(key, 1);

        Collection<ClusterNode> mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping.size() == mapping.size() : mapping;
        assert initialMapping.containsAll(mapping) : mapping;

        IgniteEx newIgnite = startGrid(4);

        awaitPartitionMapExchange();

        mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping.size() == mapping.size() : mapping;
        assert initialMapping.containsAll(mapping) : mapping;

        mapping = newIgnite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping.size() == mapping.size() : mapping;
        assert initialMapping.containsAll(mapping) : mapping;

        Set<String> stoppedNodeNames = new HashSet<>();

        ClusterNode node = mapping.iterator().next();

        stoppedNodeNames.add(nodes.get(node).name());

        nodes.get(node).close();

        nodes.remove(node);

        awaitPartitionMapExchange(true, true, null);

        mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert mapping.size() == 1 : mapping;
        assert initialMapping.containsAll(mapping);

        node = mapping.iterator().next();

        stoppedNodeNames.add(nodes.get(node).name());

        nodes.get(node).close();

        nodes.remove(node);

        awaitPartitionMapExchange();

        mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert mapping.isEmpty() : mapping;

        GridDhtPartitionTopology topology = ignite.cachex(CACHE_NAME).context().topology();

        assert topology.lostPartitions().contains(part);

        for (String nodeName : stoppedNodeNames) {
            startGrid(nodeName);
        }

        assert ignite.cluster().nodes().size() == NODE_COUNT + 1;

        mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping.size() == mapping.size() : mapping;

        for (ClusterNode n1 : initialMapping) {
            boolean found = false;

            for (ClusterNode n2 : mapping) {
                if (n2.consistentId().equals(n1.consistentId())) {
                    found = true;

                    break;
                }
            }

            assert found;
        }

        ignite.resetLostPartitions(Collections.singleton(CACHE_NAME));

        cache.put(key, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBaselineTopologyChangesFromServer() throws Exception {
        testBaselineTopologyChanges(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBaselineTopologyChangesFromClient() throws Exception {
        testBaselineTopologyChanges(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testBaselineTopologyChanges(boolean fromClient) throws Exception {
        startGrids(NODE_COUNT);

        IgniteEx ignite;

        if (fromClient) {
            client = true;

            ignite = startGrid(NODE_COUNT + 10);

            client = false;
        }
        else
            ignite = grid(0);

        ignite.active(true);

        awaitPartitionMapExchange();

        Map<ClusterNode, Ignite> nodes = new HashMap<>();

        for (int i = 0; i < NODE_COUNT; i++) {
            Ignite ig = grid(i);

            nodes.put(ig.cluster().localNode(), ig);
        }

        ignite.createCache(
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
        );

        int key = -1;

        for (int k = 0; k < 100_000; k++) {
            if (!ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(k).contains(ignite.localNode())) {
                key = k;
                break;
            }
        }

        assert key >= 0;

        Collection<ClusterNode> initialMapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping.size() == 2 : initialMapping;

        ignite.cluster().setBaselineTopology(baselineNodes(nodes.keySet()));

        Set<String> stoppedNodeNames = new HashSet<>();

        ClusterNode node = initialMapping.iterator().next();

        stoppedNodeNames.add(nodes.get(node).name());

        nodes.get(node).close();

        nodes.remove(node);

        awaitPartitionMapExchange();

        Collection<ClusterNode> mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert mapping.size() == 1 : mapping;
        assert initialMapping.containsAll(mapping);

        Set<ClusterNode> blt2 = new HashSet<>(ignite.cluster().nodes());

        ignite.cluster().setBaselineTopology(baselineNodes(blt2));

        awaitPartitionMapExchange();

        Collection<ClusterNode> initialMapping2 = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping2.size() == 2 : initialMapping2;

        Ignite newIgnite = startGrid(NODE_COUNT);

        awaitPartitionMapExchange();

        mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert mapping.size() == initialMapping2.size() : mapping;
        assert mapping.containsAll(initialMapping2);

        assert ignite.affinity(CACHE_NAME).primaryPartitions(newIgnite.cluster().localNode()).length == 0;

        Set<ClusterNode> blt3 = new HashSet<>(ignite.cluster().nodes());

        ignite.cluster().setBaselineTopology(baselineNodes(blt3));

        awaitPartitionMapExchange();

        Collection<ClusterNode> initialMapping3 = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert initialMapping3.size() == 2;

        assert ignite.affinity(CACHE_NAME).primaryPartitions(newIgnite.cluster().localNode()).length > 0;

        newIgnite = startGrid(NODE_COUNT + 1);

        awaitPartitionMapExchange();

        mapping = ignite.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert mapping.size() == initialMapping3.size() : mapping;
        assert mapping.containsAll(initialMapping3);

        assert ignite.affinity(CACHE_NAME).primaryPartitions(newIgnite.cluster().localNode()).length == 0;

        ignite.cluster().setBaselineTopology(null);

        awaitPartitionMapExchange();

        assert ignite.affinity(CACHE_NAME).primaryPartitions(newIgnite.cluster().localNode()).length > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryLeft() throws Exception {
        startGrids(NODE_COUNT);

        IgniteEx ig = grid(0);

        ig.active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache =
            ig.createCache(
                new CacheConfiguration<Integer, Integer>()
                    .setName(CACHE_NAME)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
                    .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
                    .setReadFromBackup(true)
                    .setRebalanceDelay(-1)
            );

        int key = 1;

        List<ClusterNode> affNodes = (List<ClusterNode>) ig.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert affNodes.size() == 2;

        int primaryIdx = -1;

        IgniteEx primary = null;
        IgniteEx backup = null;

        for (int i = 0; i < NODE_COUNT; i++) {
            if (grid(i).localNode().equals(affNodes.get(0))) {
                primaryIdx = i;
                primary = grid(i);
            }
            else if (grid(i).localNode().equals(affNodes.get(1)))
                backup = grid(i);
        }

        assert primary != null;
        assert backup != null;

        Integer val1 = 1;
        Integer val2 = 2;

        cache.put(key, val1);

        assertEquals(val1, primary.cache(CACHE_NAME).get(key));
        assertEquals(val1, backup.cache(CACHE_NAME).get(key));

        if (ig == primary) {
            ig = backup;

            cache = ig.cache(CACHE_NAME);
        }

        primary.close();

        assertEquals(backup.localNode(), ig.affinity(CACHE_NAME).mapKeyToNode(key));

        cache.put(key, val2);

        assertEquals(val2, backup.cache(CACHE_NAME).get(key));

        primary = startGrid(primaryIdx);

        assertEquals(backup.localNode(), ig.affinity(CACHE_NAME).mapKeyToNode(key));

        primary.cache(CACHE_NAME).rebalance().get();

        awaitPartitionMapExchange();

        assertEquals(primary.localNode(), ig.affinity(CACHE_NAME).mapKeyToNode(key));

        assertEquals(val2, primary.cache(CACHE_NAME).get(key));
        assertEquals(val2, backup.cache(CACHE_NAME).get(key));
    }

    /**
     * @throws Exception if failed.
     */
    public void testAffinityChangeNodeAdded() throws Exception {
        startGrids(NODE_COUNT);

        IgniteEx ig = grid(0);

        ig.active(true);

        IgniteCache<Object, Object> cache = ig.createCache(new CacheConfiguration<>()
            .setName(CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1));

        int parts = ig.affinity(CACHE_NAME).partitions();

        for (int i = 0; i < 5 * parts; i++)
            cache.put(i, i);

        assertEquals(4, ig.cluster().currentBaselineTopology().size());

        IgniteEx started = startGrid(NODE_COUNT);

        assertEquals(4, ig.cluster().currentBaselineTopology().size());

        for (int i = 0; i < parts; i++) {
            Collection<ClusterNode> affNodes = ig.affinity(CACHE_NAME).mapPartitionToPrimaryAndBackups(i);

            for (ClusterNode affNode : affNodes) {
                assertFalse(affNode.id().equals(started.localNode().id()));
            }
        }

        ig.cluster().setBaselineTopology(NODE_COUNT + 1);

        assertEquals(5, ig.cluster().currentBaselineTopology().size());

        awaitPartitionMapExchange(true, true, null);

        boolean found = false;

        for (int i = 0; i < parts; i++) {
            List<ClusterNode> affNodes = (List<ClusterNode>)ig.affinity(CACHE_NAME).mapPartitionToPrimaryAndBackups(i);

            for (int i1 = 0; i1 < affNodes.size(); i1++) {
                ClusterNode affNode = affNodes.get(i1);
                if (affNode.id().equals(started.localNode().id())) {
                    found = true;

                    GridDhtPartitionTopology top = started.context().cache().internalCache(CACHE_NAME)
                        .context().topology();

                    GridDhtLocalPartition locPart = top.localPartition(i, AffinityTopologyVersion.NONE, false);

                    assertNotNull(locPart);

                    if (i1 == 0) {
                        assertTrue(locPart.primary(AffinityTopologyVersion.NONE));
                        assertFalse(locPart.backup(AffinityTopologyVersion.NONE));
                    }
                    else {
                        assertTrue(locPart.backup(AffinityTopologyVersion.NONE));
                        assertFalse(locPart.primary(AffinityTopologyVersion.NONE));
                    }
                }
            }
        }

        assertTrue(found);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryLeftAndClusterRestart() throws Exception {
        startGrids(NODE_COUNT);

        IgniteEx ig = grid(0);

        ig.active(true);

        IgniteCache<Integer, Integer> cache =
            ig.createCache(
                new CacheConfiguration<Integer, Integer>()
                    .setName(CACHE_NAME)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
                    .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
                    .setReadFromBackup(true)
                    .setRebalanceDelay(-1)
            );

        int key = 1;

        List<ClusterNode> affNodes = (List<ClusterNode>) ig.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assert affNodes.size() == 2;

        int primaryIdx = -1;
        int backupIdx = -1;

        IgniteEx primary = null;
        IgniteEx backup = null;

        for (int i = 0; i < NODE_COUNT; i++) {
            if (grid(i).localNode().equals(affNodes.get(0))) {
                primaryIdx = i;
                primary = grid(i);
            }
            else if (grid(i).localNode().equals(affNodes.get(1))) {
                backupIdx = i;
                backup = grid(i);
            }
        }

        assert primary != null;
        assert backup != null;

        Integer val1 = 1;
        Integer val2 = 2;

        cache.put(key, val1);

        assertEquals(val1, primary.cache(CACHE_NAME).get(key));
        assertEquals(val1, backup.cache(CACHE_NAME).get(key));

        if (ig == primary) {
            ig = backup;

            cache = ig.cache(CACHE_NAME);
        }

        stopGrid(primaryIdx, false);

        ig.context().cache().context().exchange().affinityReadyFuture(new AffinityTopologyVersion(5, 0)).get();

        assertEquals(backup.localNode(), ig.affinity(CACHE_NAME).mapKeyToNode(key));

        cache.put(key, val2);

        assertEquals(val2, backup.cache(CACHE_NAME).get(key));

        stopAllGrids(false);

        startGrids(NODE_COUNT);

        ig = grid(0);
        primary = grid(primaryIdx);
        backup = grid(backupIdx);

        boolean activated = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < NODE_COUNT; i++)
                  if (!grid(i).active())
                      return false;

                return true;
            }
        }, 10_000);

        assert activated;

        info("Primary: " + primary);
        info("Backup: " + backup);

        GridCacheAdapter<Object, Object> intCache = primary.context().cache().internalCache(CACHE_NAME);
        GridDhtPartitionTopology top = intCache.context().group().topology();

        GridDhtLocalPartition locPart = top.localPartition(intCache.affinity().partition(key),
            new AffinityTopologyVersion(4, 1), false);

        info("Attempting to read local partition: " + locPart);

        // Check read from an outdated primary node when rebalancing is not finished yet.
        assertEquals(val2, primary.cache(CACHE_NAME).get(key));
        assertEquals(val2, backup.cache(CACHE_NAME).get(key));

        primary.cache(CACHE_NAME).rebalance().get();

        affNodes = (List<ClusterNode>) ig.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(key);

        assertEquals(primary.localNode(), affNodes.get(0));
        assertEquals(backup.localNode(), affNodes.get(1));

        assertEquals(val2, primary.cache(CACHE_NAME).get(key));
        assertEquals(val2, backup.cache(CACHE_NAME).get(key));
    }

    /** */
    private Collection<BaselineNode> baselineNodes(Collection<ClusterNode> clNodes) {
        Collection<BaselineNode> res = new ArrayList<>(clNodes.size());

        res.addAll(clNodes);

        return res;
    }
}
