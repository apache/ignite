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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 *
 */
public class CachePartitionStateTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClientMode(client);

        if (ccfg != null) {
            cfg.setCacheConfiguration(ccfg);

            ccfg = null;
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState1_1() throws Exception {
        partitionState1(0, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState1_2() throws Exception {
        partitionState1(1, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState1_2_NoCacheOnCoordinator() throws Exception {
        partitionState1(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState1_3() throws Exception {
        partitionState1(100, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState2_1() throws Exception {
        partitionState2(0, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState2_2() throws Exception {
        partitionState2(1, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState2_2_NoCacheOnCoordinator() throws Exception {
        partitionState2(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionState2_3() throws Exception {
        partitionState2(100, true);
    }

    /**
     * @param backups Number of backups.
     * @param crdAffNode If {@code false} cache is not created on coordinator.
     * @throws Exception If failed.
     */
    private void partitionState1(int backups, boolean crdAffNode) throws Exception {
        startGrids(3);

        blockSupplySend(DEFAULT_CACHE_NAME);

        CacheConfiguration ccfg = cacheConfiguration(DEFAULT_CACHE_NAME, backups);

        if (!crdAffNode)
            ccfg.setNodeFilter(new TestCacheNodeExcludingFilter(getTestIgniteInstanceName(0)));

        ignite(1).createCache(ccfg);

        AffinityAssignment assign0 =
            grid(1).context().cache().internalCache(DEFAULT_CACHE_NAME).context().affinity().assignment(
                new AffinityTopologyVersion(3, 1));

        awaitPartitionMapExchange();

        checkPartitionsState(assign0, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);

        client = true;

        Ignite clientNode = startGrid(4);

        checkPartitionsState(assign0, DEFAULT_CACHE_NAME, OWNING);

        clientNode.cache(DEFAULT_CACHE_NAME);

        checkPartitionsState(assign0, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);

        client = false;

        startGrid(5);

        checkRebalance(DEFAULT_CACHE_NAME, false);

        for (int i = 0; i < 3; i++)
            checkNodePartitions(assign0, ignite(i).cluster().localNode(), DEFAULT_CACHE_NAME, OWNING);

        AffinityAssignment assign1 =
            grid(1).context().cache().internalCache(DEFAULT_CACHE_NAME).context().affinity().assignment(
                new AffinityTopologyVersion(5, 0));

        checkNodePartitions(assign1, ignite(5).cluster().localNode(), DEFAULT_CACHE_NAME, MOVING);

        stopBlock();

        awaitPartitionMapExchange();

        AffinityAssignment assign2 =
            grid(1).context().cache().internalCache(DEFAULT_CACHE_NAME).context().affinity().assignment(
                new AffinityTopologyVersion(5, 1));

        checkPartitionsState(assign2, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);

        if (!crdAffNode)
            ignite(0).cache(DEFAULT_CACHE_NAME);

        checkPartitionsState(assign2, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);

        startGrid(6);

        awaitPartitionMapExchange();

        AffinityAssignment assign3 =
            grid(1).context().cache().internalCache(DEFAULT_CACHE_NAME).context().affinity().assignment(
                new AffinityTopologyVersion(6, 1));

        checkPartitionsState(assign3, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);
    }

    /**
     * @param backups Number of backups.
     * @param crdAffNode If {@code false} cache is not created on coordinator.
     * @throws Exception If failed.
     */
    private void partitionState2(int backups, boolean crdAffNode) throws Exception {
        startGrids(3);

        blockSupplySend(DEFAULT_CACHE_NAME);

        ccfg = cacheConfiguration(DEFAULT_CACHE_NAME, backups);

        if (!crdAffNode)
            ccfg.setNodeFilter(new TestCacheNodeExcludingFilter(getTestIgniteInstanceName(0)));

        startGrid(4);

        AffinityAssignment assign0 =
            grid(1).context().cache().internalCache(DEFAULT_CACHE_NAME).context().affinity().assignment(
                new AffinityTopologyVersion(4, 0));

        checkPartitionsState(assign0, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);

        if (!crdAffNode)
            ignite(0).cache(DEFAULT_CACHE_NAME);

        checkPartitionsState(assign0, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);

        stopBlock();

        startGrid(5);

        AffinityAssignment assign1 =
            grid(1).context().cache().internalCache(DEFAULT_CACHE_NAME).context().affinity().assignment(
                new AffinityTopologyVersion(5, 1));

        awaitPartitionMapExchange();

        checkPartitionsState(assign1, DEFAULT_CACHE_NAME, OWNING);

        checkRebalance(DEFAULT_CACHE_NAME, true);
    }

    /**
     * @param assign Assignments.
     * @param cacheName Cache name.
     * @param expState Expected state.
     */
    private void checkPartitionsState(AffinityAssignment assign, String cacheName, GridDhtPartitionState expState) {
        for (Ignite node : G.allGrids())
            checkNodePartitions(assign, node.cluster().localNode(), cacheName, expState);
    }

    /**
     * @param assign Assignments.
     * @param clusterNode Node.
     * @param cacheName Cache name.
     * @param expState Expected partitions state.
     */
    private void checkNodePartitions(AffinityAssignment assign,
        ClusterNode clusterNode,
        String cacheName,
        GridDhtPartitionState expState)
    {
        Affinity<Object> aff = ignite(0).affinity(cacheName);

        Set<Integer> nodeParts = new HashSet<>();

        nodeParts.addAll(assign.primaryPartitions(clusterNode.id()));
        nodeParts.addAll(assign.backupPartitions(clusterNode.id()));

        log.info("Test state [node=" + clusterNode.id() +
            ", cache=" + cacheName +
            ", parts=" + nodeParts.size() +
            ", state=" + expState + ']');

        if (grid(0).context().discovery().cacheAffinityNode(clusterNode, cacheName))
            assertFalse(nodeParts.isEmpty());

        boolean check = false;

        for (Ignite node : G.allGrids()) {
            GridCacheAdapter cache =
                ((IgniteKernal)node).context().cache().internalCache(cacheName);

            if (cache != null) {
                check = true;

                GridDhtPartitionTopology top = cache.context().topology();

                GridDhtPartitionMap partsMap = top.partitions(clusterNode.id());

                for (int p = 0; p < aff.partitions(); p++) {
                    if (nodeParts.contains(p)) {
                        assertNotNull(partsMap);

                        GridDhtPartitionState state = partsMap.get(p);

                        assertEquals("Unexpected state [checkNode=" + clusterNode.id() +
                            ", node=" + node.name() +
                            ", state=" + state + ']',
                            expState, partsMap.get(p));
                    }
                    else {
                        if (partsMap != null) {
                            GridDhtPartitionState state = partsMap.get(p);

                            assertTrue("Unexpected state [checkNode=" + clusterNode.id() +
                                    ", node=" + node.name() +
                                    ", state=" + state + ']',
                                state == null || state == EVICTED);
                        }
                    }
                }
            }
            else {
                assertEquals(0, aff.primaryPartitions(((IgniteKernal)node).localNode()).length);
                assertEquals(0, aff.backupPartitions(((IgniteKernal)node).localNode()).length);
            }
        }

        assertTrue(check);
    }

    /**
     * @param cacheName Cache name.
     * @param expDone Expected rebalance finish flag.
     */
    private void checkRebalance(String cacheName, boolean expDone) {
        for (Ignite node : G.allGrids()) {
            IgniteKernal node0 = (IgniteKernal)node;

            GridCacheAdapter cache = node0.context().cache().internalCache(cacheName);

            AffinityTopologyVersion topVer = node0.context().cache().context().exchange().readyAffinityVersion();

            if (cache != null)
                assertEquals(expDone, cache.context().topology().rebalanceFinished(topVer));
            else
                node0.context().discovery().cacheAffinityNode(node0.localNode(), cacheName);
        }
    }

    /**
     * @param cacheName Cache name.
     */
    private void blockSupplySend(String cacheName) {
        for (Ignite node : G.allGrids())
            blockSupplySend(TestRecordingCommunicationSpi.spi(node), cacheName);
    }

    /**
     * @param spi SPI.
     * @param cacheName Cache name.
     */
    private void blockSupplySend(TestRecordingCommunicationSpi spi, final String cacheName) {
        final int grpId = CU.cacheId(cacheName);

        spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg.getClass().equals(GridDhtPartitionSupplyMessage.class) &&
                    ((GridDhtPartitionSupplyMessage)msg).groupId() == grpId;
            }
        });
    }

    /**
     *
     */
    private void stopBlock() {
        for (Ignite node : G.allGrids())
            TestRecordingCommunicationSpi.spi(node).stopBlock();
    }

    /**
     * @param name Cache name.
     * @param backups Backups number.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, int backups) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);

        return ccfg;
    }
}
