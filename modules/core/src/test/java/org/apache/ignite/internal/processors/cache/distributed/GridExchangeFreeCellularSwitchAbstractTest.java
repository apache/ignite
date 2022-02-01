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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class GridExchangeFreeCellularSwitchAbstractTest extends GridCommonAbstractTest {
    /** Partitioned cache name. */
    protected static final String PART_CACHE_NAME = "partitioned";

    /** Replicated cache name. */
    protected static final String REPL_CACHE_NAME = "replicated";

    /** */
    protected final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = new DataRegionConfiguration();

        drCfg.setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /**
     *
     */
    private CacheConfiguration<?, ?>[] cacheConfiguration() {
        CacheConfiguration<?, ?> partitionedCcfg = new CacheConfiguration<>();

        partitionedCcfg.setName(PART_CACHE_NAME);
        partitionedCcfg.setWriteSynchronizationMode(FULL_SYNC);
        partitionedCcfg.setBackups(2);
        partitionedCcfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        partitionedCcfg.setAffinity(new Map6PartitionsTo6NodesTo2CellsAffinityFunction());

        CacheConfiguration<?, ?> replicatedCcfg = new CacheConfiguration<>();

        replicatedCcfg.setName(REPL_CACHE_NAME);
        replicatedCcfg.setWriteSynchronizationMode(FULL_SYNC);
        replicatedCcfg.setCacheMode(CacheMode.REPLICATED);
        replicatedCcfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        return new CacheConfiguration[] {partitionedCcfg, replicatedCcfg};
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    protected void awaitForSwitchOnNodeLeft(Ignite failed) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(
            () -> {
                for (Ignite ignite : G.allGrids()) {
                    if (ignite == failed)
                        continue;

                    GridDhtPartitionsExchangeFuture fut =
                        ((IgniteEx)ignite).context().cache().context().exchange().lastTopologyFuture();

                    if (!fut.exchangeId().isLeft())
                        return false;
                }

                return true;
            }, 10000));
    }

    /**
     *
     */
    protected void blockRecoveryMessages() {
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg.getClass().equals(GridCacheTxRecoveryRequest.class);
                }
            });
        }
    }

    /**
     *
     */
    protected void checkTransactionsCount(
        Ignite origNode, int origCnt,
        List<Ignite> brokenCellNodes, int brokenCellCnt,
        List<Ignite> aliveCellNodes, int aliveCellCnt,
        Set<GridCacheVersion> vers) {
        Function<Ignite, Collection<GridCacheVersion>> txs = ignite -> {
            Collection<IgniteInternalTx> active = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            // Transactions originally started at backups will be presented as single element.
            return active.stream()
                .map(IgniteInternalTx::nearXidVersion)
                .filter(ver -> vers == null || vers.contains(ver))
                .collect(Collectors.toSet());
        };

        long till = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

        while (true) {
            try {
                if (origNode != null)
                    assertEquals(origCnt, txs.apply(origNode).size());

                for (Ignite brokenCellNode : brokenCellNodes)
                    if (brokenCellNode != origNode)
                        assertEquals(brokenCellCnt, txs.apply(brokenCellNode).size());

                for (Ignite aliveCellNode : aliveCellNodes)
                    if (aliveCellNode != origNode)
                        assertEquals(aliveCellCnt, txs.apply(aliveCellNode).size());

                break;
            }
            catch (Throwable err) {
                if (System.nanoTime() > till)
                    throw err;
            }
        }

    }

    /**
     *
     */
    protected CellularCluster resolveCellularCluster(int nodes, TransactionCoordinatorNode startFrom) throws Exception {
        Ignite failed = G.allGrids().get(new Random().nextInt(nodes));

        Integer cellKey = primaryKey(failed.getOrCreateCache(PART_CACHE_NAME));

        List<Ignite> brokenCellNodes = backupNodes(cellKey, PART_CACHE_NAME);
        List<Ignite> aliveCellNodes = new ArrayList<>(G.allGrids());

        aliveCellNodes.remove(failed);
        aliveCellNodes.removeAll(brokenCellNodes);

        assertTrue(Collections.disjoint(brokenCellNodes, aliveCellNodes));
        assertEquals(nodes / 2 - 1, brokenCellNodes.size());
        assertEquals(nodes / 2, aliveCellNodes.size());

        Ignite orig;

        switch (startFrom) {
            case FAILED:
                orig = failed;

                break;

            case BROKEN_CELL:
                orig = brokenCellNodes.get(new Random().nextInt(brokenCellNodes.size()));

                break;

            case ALIVE_CELL:
                orig = aliveCellNodes.get(new Random().nextInt(aliveCellNodes.size()));

                break;

            case CLIENT:
                orig = startClientGrid();

                break;

            default:
                throw new UnsupportedOperationException();
        }

        return new CellularCluster(orig, failed, brokenCellNodes, aliveCellNodes);
    }

    /**
     *
     */
    protected static class Map6PartitionsTo6NodesTo2CellsAffinityFunction extends RendezvousAffinityFunction {
        /**
         * Default constructor.
         */
        public Map6PartitionsTo6NodesTo2CellsAffinityFunction() {
            super(false, 6);
        }

        /**
         * {@inheritDoc}
         */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(6);

            int backups = affCtx.backups();

            assert backups == 2;

            if (affCtx.currentTopologySnapshot().size() == 6) {
                List<ClusterNode> p0 = new ArrayList<>();
                List<ClusterNode> p1 = new ArrayList<>();
                List<ClusterNode> p2 = new ArrayList<>();
                List<ClusterNode> p3 = new ArrayList<>();
                List<ClusterNode> p4 = new ArrayList<>();
                List<ClusterNode> p5 = new ArrayList<>();

                // Cell 1.
                p0.add(affCtx.currentTopologySnapshot().get(0));
                p0.add(affCtx.currentTopologySnapshot().get(1));
                p0.add(affCtx.currentTopologySnapshot().get(2));

                p1.add(affCtx.currentTopologySnapshot().get(2));
                p1.add(affCtx.currentTopologySnapshot().get(0));
                p1.add(affCtx.currentTopologySnapshot().get(1));

                p2.add(affCtx.currentTopologySnapshot().get(1));
                p2.add(affCtx.currentTopologySnapshot().get(2));
                p2.add(affCtx.currentTopologySnapshot().get(0));

                // Cell 2.
                p3.add(affCtx.currentTopologySnapshot().get(3));
                p3.add(affCtx.currentTopologySnapshot().get(4));
                p3.add(affCtx.currentTopologySnapshot().get(5));

                p4.add(affCtx.currentTopologySnapshot().get(5));
                p4.add(affCtx.currentTopologySnapshot().get(3));
                p4.add(affCtx.currentTopologySnapshot().get(4));

                p5.add(affCtx.currentTopologySnapshot().get(4));
                p5.add(affCtx.currentTopologySnapshot().get(5));
                p5.add(affCtx.currentTopologySnapshot().get(3));

                res.add(p0);
                res.add(p1);
                res.add(p2);
                res.add(p3);
                res.add(p4);
                res.add(p5);
            }

            return res;
        }
    }

    /**
     * Specifies node starts the transaction (originating node).
     */
    protected enum TransactionCoordinatorNode {
        /** Failed. */
        FAILED,

        /**Broken cell. */
        BROKEN_CELL,

        /** Alive cell. */
        ALIVE_CELL,

        /** Client. */
        CLIENT
    }

    /**
     *
     */
    protected static class CellularCluster {
        /** Originating node. */
        public Ignite orig;

        /** Failed node. */
        public Ignite failed;

        /** Broken cell's nodes. */
        public List<Ignite> brokenCellNodes;

        /** Alive cell's nodes. */
        public List<Ignite> aliveCellNodes;

        /**
         *
         */
        public CellularCluster(Ignite orig, Ignite failed, List<Ignite> brokenCellNodes,
            List<Ignite> aliveCellNodes) {
            this.orig = orig;
            this.failed = failed;
            this.brokenCellNodes = brokenCellNodes;
            this.aliveCellNodes = aliveCellNodes;
        }
    }
}
