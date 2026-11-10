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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_CENTER_ID;

/**
 * Tests MDC disaster-recovery behavior under a LIVE network partition: the two data
 * centers stay up but lose sight of each other, and a topology validator makes the
 * isolated minority data center read-only while the majority keeps serving writes.
 *
 * <p>Unlike {@link MultiDcRebalancingTest} (where a DC node is stopped and writes nothing
 * while away), here both halves stay alive during the partition. Segmentation is achieved
 * by faking discovery socket timeouts across the DC boundary (each half detects the other
 * as failed) plus blocking cross-DC communication messages, reusing
 * {@link IgniteCacheTopologySplitAbstractTest}.
 */
public class MultiDcSplitBrainRebalanceTest extends IgniteCacheTopologySplitAbstractTest {
    /** */
    private static final String DC1 = "DC1";

    /** */
    private static final String DC2 = "DC2";

    /** */
    private static final int SEG_DC1 = 0;

    /** */
    private static final int SEG_DC2 = 1;

    /** Per-DC node layout: {0,1,2} -> DC1 (majority), {3} -> DC2 (minority). */
    private static final int[] NODE_DC = {0, 0, 0, 1};

    /** Base discovery port; each node takes base + idx. */
    private static final int DISCO_PORT_BASE = TcpDiscoverySpi.DFLT_PORT;

    /** discovery port -> segment (0 = DC1, 1 = DC2). Filled on node start. */
    private final Map<Integer, Integer> portToSeg = new ConcurrentHashMap<>();

    /** */
    private boolean pds;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        int idx = Integer.parseInt(gridName.replaceAll("\\D+", ""));
        cfg.setConsistentId(gridName);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoSpi.setLocalPort(DISCO_PORT_BASE + idx);
        portToSeg.put(DISCO_PORT_BASE + idx, NODE_DC[idx] == 0 ? SEG_DC1 : SEG_DC2);

        List<String> discoAddrs = new ArrayList<>();
        for (int i = 0; i < NODE_DC.length; i++) {
            String addr = "127.0.0.1:" + (DISCO_PORT_BASE + i);
            discoAddrs.add(addr);
        }
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setShared(true);
        ipFinder.setAddresses(discoAddrs);
        discoSpi.setIpFinder(ipFinder);

        cfg.setUserAttributes(F.asMap(IGNITE_DATA_CENTER_ID, NODE_DC[idx] == 0 ? DC1 : DC2));

        DataRegionConfiguration dfltRgn = new DataRegionConfiguration();
        dfltRgn.setPersistenceEnabled(pds);
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(dfltRgn);
        cfg.setDataStorageConfiguration(dsCfg);
        cfg.setCommunicationSpi(new DemandRecordingCommSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int segment(ClusterNode node) {
        return DC2.equals(node.dataCenterId()) ? SEG_DC2 : SEG_DC1;
    }

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        Integer locSeg = portToSeg.get(locPort);
        Integer rmtSeg = portToSeg.get(rmtPort);
        if (locSeg == null || rmtSeg == null)
            return false;
        return !locSeg.equals(rmtSeg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        portToSeg.clear();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * Disaster recovery: a network partition between the data centers makes the minority
     * DC read-only while the majority DC keeps serving writes. The cache carries a
     * topology validator that keeps it operative only while a node of the main DC (DC1)
     * is visible in the topology; the isolated minority (DC2) no longer sees DC1, so the
     * cache turns inoperative there and its writes are rejected, whereas the majority
     * (DC1) stays writable and preserves its data.
     *
     * <p>Scenario: 4 nodes, DC1={node0,node1,node2} (majority, main DC), DC2={node3}
     * (minority). After 1000 base entries the network is split; DC1 keeps writing, DC2
     * writes are rejected.
     */
    @Test
    public void testNetworkPartitionMinorityBecomesReadOnly() throws Exception {
        IgniteCache<Object, Object> cache0 = startSplitBrainCluster(new MainDcOnlyTopologyValidator());

        splitAndWait();

        cache0.put(1000, 1000);
        assertEquals(1000, cache0.get(1000));

        assertFalse("Minority DC2 must reject writes while partitioned",
            tryPut(grid(3), 2000, 2000));

        unsplit();

        logRebalanceAfterHeal("MainDcOnly");

        for (int i = 0; i <= 1000; i += 500) {
            Object v = cache0.get(i);
            assertEquals(i, v);
        }
    }

    /**
     * Starts a 4-node MDC cluster (node0,1,2 in DC1, node3 in DC2) in ACTIVE state with
     * baseline auto-adjustment disabled, creates a cache with backups=1 and the given
     * topology validator (or none), writes 1000 base entries, and forces a checkpoint.
     *
     * @param validator Topology validator to apply, or {@code null} for no validator.
     * @return The cache on node0.
     */
    private IgniteCache<Object, Object> startSplitBrainCluster(TopologyValidator validator) throws Exception {
        pds = true;

        startGrid(0);
        startGrid(3);
        grid(0).cluster().state(ClusterState.ACTIVE);
        grid(0).cluster().baselineAutoAdjustEnabled(false);

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cacheCfg.setBackups(1);
        if (validator != null)
            cacheCfg.setTopologyValidator(validator);
        IgniteCache<Object, Object> cache = grid(0).createCache(cacheCfg);

        startGrid(1);
        startGrid(2);
        awaitServers(4, grid(0));

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        forceCheckpoint();

        return cache;
    }

    /** */
    private boolean tryPut(IgniteEx ignite, int key, int val) {
        try {
            IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);
            cache.put(key, val);

            return true;
        }
        catch (Exception e) {
            log.info(">>> Put rejected [node=" + ignite.name() + ", err=" + e.getMessage() + "]");

            return false;
        }
    }

    /**
     * After unsplit, logs the actual state and the rebalance demand counts per node.
     *
     * <p>Documented limitation: {@code splitAndWait}/{@code unsplit} cannot merge two
     * half-rings back — once a half detects the loss of the other and processes
     * {@code EVT_NODE_FAILED}, it stops reconnecting to those nodes. No new topology
     * version is created across the halves, so no cross-DC rebalance demand is sent
     * ({@code rebalanceMsgCnt == 0}). True split-brain merge with state-synchronization
     * and rebalance is covered at the ducktape level (netem + control utility).
     */
    private void logRebalanceAfterHeal(String testLabel) {
        for (Ignite ig : G.allGrids()) {
            IgniteConfiguration cfg = ig.configuration();
            DemandRecordingCommSpi spi = (DemandRecordingCommSpi)cfg.getCommunicationSpi();

            IgniteCluster cluster = ig.cluster();
            ClusterGroup srvGrp = cluster.forServers();
            Collection<? extends ClusterNode> srvNodes = srvGrp.nodes();
            int srvCnt = srvNodes.size();

            log.info(">>> [" + testLabel + "] node=" + ig.name()
                + ", servers_in_topo=" + srvCnt
                + ", rebalanceMsgCnt=" + spi.rebalanceMsgCnt);
        }
    }

    /**
     * Waits until the given node's cluster topology includes the expected number of
     * server nodes. Returns as soon as the condition holds (30s safety cap).
     */
    private void awaitServers(int expected, IgniteEx ignite) throws Exception {
        IgniteCluster cluster = ignite.cluster();
        boolean done = GridTestUtils.waitForCondition(() -> {
            ClusterGroup srvGrp = cluster.forServers();
            Collection<? extends ClusterNode> srvNodes = srvGrp.nodes();
            return srvNodes.size() == expected;
        }, 30_000);
        assertTrue("Topology did not reach " + expected + " servers on " + ignite.name(), done);
    }

    /**
     * Verifies that with a validator requiring both DCs, the partition makes BOTH halves
     * read-only: neither can see the other DC, so neither satisfies the validator.
     *
     * <p>Scenario: 4 nodes, DC1={0,1,2}, DC2={3}. Validator returns false unless at
     * least one node from each DC is in the topology. After split, both sides reject
     * writes.
     */
    @Test
    public void testNetworkPartitionBothHalvesReadOnly() throws Exception {
        startSplitBrainCluster(new BothDcsTopologyValidator());

        splitAndWait();

        assertFalse("Majority DC1 must reject writes without DC2",
            tryPut(grid(0), 1000, 1000));

        assertFalse("Minority DC2 must reject writes without DC1",
            tryPut(grid(3), 2000, 2000));

        unsplit();

        logRebalanceAfterHeal("BothDcsRequired");
    }

    /**
     * Verifies that with a quorum validator (min server nodes), the majority keeps
     * writing while the minority loses quorum and becomes read-only.
     *
     * <p>Scenario: 4 nodes, DC1={0,1,2} (3 nodes >= threshold=3), DC2={3} (1 node < 3).
     * After split, DC1 passes the quorum check, DC2 does not.
     */
    @Test
    public void testNetworkPartitionMajorityKeepsQuorum() throws Exception {
        IgniteCache<Object, Object> cache0 = startSplitBrainCluster(new MinServerCountTopologyValidator(3));

        splitAndWait();

        cache0.put(1000, 1000);
        assertEquals(1000, cache0.get(1000));

        assertFalse("Minority DC2 must reject writes without quorum",
            tryPut(grid(3), 2000, 2000));

        unsplit();

        logRebalanceAfterHeal("MinServerCount3");
    }

    /**
     * Verifies that without a topology validator, BOTH halves stay writable during the
     * partition (uncontrolled split-brain). Each half elects its own coordinator and
     * accepts writes independently.
     *
     * <p>Scenario: 4 nodes, DC1={0,1,2}, DC2={3}. No validator set. After split, both
     * DC1 and DC2 write successfully.
     */
    @Test
    public void testNetworkPartitionNoValidatorBothWritable() throws Exception {
        IgniteCache<Object, Object> cache0 = startSplitBrainCluster(null);

        splitAndWait();

        cache0.put(1000, 1000);
        assertEquals(1000, cache0.get(1000));

        IgniteCache<Object, Object> cache3 = grid(3).cache(DEFAULT_CACHE_NAME);

        cache3.put(2000, 2000);
        assertEquals(2000, cache3.get(2000));

        unsplit();

        logRebalanceAfterHeal("NoValidator");
    }

    /**
     * Keeps the cache operative only while at least one server node of the main data
     * center (DC1) is present in the topology. When a partition isolates the minority
     * DC2 from DC1, the cache becomes inoperative on the minority and rejects writes.
     */
    private static class MainDcOnlyTopologyValidator implements TopologyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            for (ClusterNode node : nodes) {
                if (!node.isClient() && DC1.equals(node.dataCenterId()))
                    return true;
            }

            return false;
        }
    }

    /**
     * Keeps the cache operative only while server nodes from BOTH DCs are present.
     * When any DC is isolated, the validator fails on both sides.
     */
    private static class BothDcsTopologyValidator implements TopologyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            boolean hasDc1 = false;
            boolean hasDc2 = false;

            for (ClusterNode node : nodes) {
                if (node.isClient())
                    continue;
                String dc = node.dataCenterId();
                if (DC1.equals(dc))
                    hasDc1 = true;
                if (DC2.equals(dc))
                    hasDc2 = true;
            }

            return hasDc1 && hasDc2;
        }
    }

    /**
     * Keeps the cache operative only while at least {@code minCnt} server nodes are
     * present in the topology.
     */
    private static class MinServerCountTopologyValidator implements TopologyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final int minCnt;

        /** */
        MinServerCountTopologyValidator(int minCnt) {
            this.minCnt = minCnt;
        }

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            int serverCnt = 0;

            for (ClusterNode node : nodes) {
                if (!node.isClient())
                    serverCnt++;
            }

            return serverCnt >= minCnt;
        }
    }

    /**
     * Communication SPI that records rebalance demand messages while inheriting the
     * message-blocking behavior used for segmentation.
     */
    private static class DemandRecordingCommSpi extends TestRecordingCommunicationSpi {
        /** */
        private volatile int rebalanceMsgCnt;

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackClosure
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage)
                rebalanceMsgCnt++;
            super.sendMessage(node, msg, ackClosure);
        }
    }
}

