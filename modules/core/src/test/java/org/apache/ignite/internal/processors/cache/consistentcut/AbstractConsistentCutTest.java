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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutTest.TestConsistentCutManager.cutMgr;

/** Base class for testing Consistency Cut algorithm. */
public abstract class AbstractConsistentCutTest extends GridCommonAbstractTest {
    /** */
    protected static final String CACHE = "CACHE";

    /** */
    private final Random rnd = new Random();

    /** nodeIdx -> Consistent Node ID. */
    private final List<UUID> consistentIds = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDataRegionConfigurations(new DataRegionConfiguration()
                .setName("consistent-cut-persist")
                .setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(backups())
                .setDataRegionName("consistent-cut-persist"));

        cfg.setCommunicationSpi(new LogCommunicationSpi());

        cfg.setPluginProviders(new TestConsistentCutManagerPluginProvider());

        int idx = getTestIgniteInstanceIndex(instanceName);

        if (consistentIds.size() > idx)
            cfg.setConsistentId(consistentIds.get(idx));
        else {
            cfg.setConsistentId(UUID.randomUUID());

            consistentIds.add((UUID)cfg.getConsistentId());
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);

        startClientGrid(nodes());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @return Number of server nodes.
     */
    protected abstract int nodes();

    /**
     * @return Number of backups for cache.
     */
    protected abstract int backups();

    /**
     * Provides a key that for an existing partitioning schema match specified primary and backup node.
     *
     * @param primary Node that is primary for a lookup key.
     * @param backup Node that is backup for a lookup key. {@code null} if cnt of backups is 0 for cache.
     * @return Key that matches specified primary and backup nodes.
     */
    protected final int key(String cache, ClusterNode primary, @Nullable ClusterNode backup) {
        assert backup != null || backups() == 0;

        Affinity<Integer> aff = grid(0).affinity(cache);

        int key = rnd.nextInt();

        while (true) {
            if (aff.isPrimary(primary, key) && (backup == null || aff.isBackup(backup, key)))
                return key;

            key++;
        }
    }

    /**
     * @param cuts Amount of Consistent Cut to await.
     */
    protected void awaitConsistentCuts(int cuts) throws Exception {
        for (int i = 1; i <= cuts; i++) {
            Thread.sleep(100);

            cutMgr(grid(0)).triggerConsistentCutOnCluster().get(getTestTimeout());

            log.info("Consistent Cut finished: " + i);
        }
    }

    /**
     * Stops Ignite cluster and prepares description (nodeId, consistenceId, compactId) of stopped Ignite nodes.
     *
     * @return { nodeId -> compactId }.
     */
    protected Map<Integer, Short> stopCluster() throws Exception {
        flushWalAllGrids();

        Map<Object, Short> consistentMapping = grid(0).context().discovery().discoCache().state().baselineTopology()
            .consistentIdMapping();

        Map<Integer, Short> m = new HashMap<>();

        for (int i = 0; i < nodes(); i++) {
            short compactId = consistentMapping.get(consistentIds.get(i));

            m.put(i, compactId);
        }

        stopAllGrids();

        return m;
    }

    /**
     * Checks WALs for correct Consistency Cut.
     *
     * @param txNearNode Collection of transactions was run within a test. Key is transaction ID, value is near node ID.
     * @param cuts       Number of Consistent Cuts was run within a test.
     */
    protected void checkWalsConsistency(Map<IgniteUuid, Integer> txNearNode, int cuts) throws Exception {
        Map<Integer, Short> top = stopCluster();

        List<ConsistentCutWalReader> states = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            log.info("Check WAL for node " + i);

            ConsistentCutWalReader reader = new ConsistentCutWalReader(i, walIter(i), log, txNearNode, top);

            states.add(reader);

            reader.read();

            // Skip all tests for input testCase and blocks.
            if (reader.cuts.isEmpty()) {
                assertTrue(txNearNode.isEmpty());

                return;
            }

            int expCuts = reader.cuts.get(reader.cuts.size() - 1).completed ? cuts : cuts + 1;

            assertEquals(expCuts, reader.cuts.size());
        }

        // Transaction ID -> (cutId, nodeId).
        Map<IgniteUuid, T2<Integer, Integer>> txMap = new HashMap<>();

        // Includes incomplete state also.
        for (int cutId = 0; cutId < cuts + 1; cutId++) {
            for (int nodeId = 0; nodeId < nodes(); nodeId++) {
                // Skip if the latest cut wasn't INCOMPLETE.
                if (states.get(nodeId).cuts.size() == cutId)
                    continue;

                ConsistentCutWalReader.NodeConsistentCutState state = states.get(nodeId).cuts.get(cutId);

                for (IgniteUuid xid: state.committedTx) {
                    T2<Integer, Integer> prev = txMap.put(xid, new T2<>(state.num, nodeId));

                    if (prev != null) {
                        assertTrue("Transaction miscutted: " + xid + ". Node" + prev.get2() + "=" + prev.get1() +
                            ". Node" + nodeId + "=" + state.num, prev.get1() == state.num);
                    }
                }
            }
        }

        assertEquals(txNearNode.size(), txMap.size());
    }

    /** */
    protected void flushWalAllGrids() throws Exception {
        for (Ignite ign: G.allGrids()) {
            IgniteWriteAheadLogManager walMgr = ((IgniteEx)ign).context().cache().context().wal();

            if (walMgr != null)
                walMgr.flush(null, true);
        }
    }

    /** Get iterator over WAL. */
    protected WALIterator walIter(int nodeIdx) throws Exception {
        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        String subfolderName = U.maskForFileName(consistentIds.get(nodeIdx).toString());

        File wal = Paths.get(workDir).resolve(DataStorageConfiguration.DFLT_WAL_PATH).resolve(subfolderName).toFile();
        File archive = Paths.get(workDir).resolve(DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH).resolve(subfolderName).toFile();

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(wal, archive);

        return factory.iterator(params);
    }

    /** */
    private static class TestConsistentCutManagerPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TestConsistentCutManagerPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (ConsistentCutManager.class.equals(cls))
                return (T)new TestConsistentCutManager();

            return null;
        }
    }

    /** ConsistentCutManager with possibility to disable schedulling Consistent Cuts manually. */
    protected static class TestConsistentCutManager extends ConsistentCutManager {
        /** */
        static TestConsistentCutManager cutMgr(IgniteEx ign) {
            return (TestConsistentCutManager)ign.context().cache().context().consistentCutMgr();
        }

        /** */
        boolean consistentCutFinished(long ts) {
            if (CONSISTENT_CUT.get(this) != null)
                return false;

            return lastSeenMarker().timestamp() == ts && clusterCutFut == null;
        }
    }

    /** Logs TX messages between nodes. */
    protected static class LogCommunicationSpi extends TestRecordingCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (log.isDebugEnabled() && txMessage(msg))
                logTxMessage(node, msg);

            super.sendMessage(node, msg, ackC);
        }

        /** */
        private void logTxMessage(ClusterNode node, Message msg) {
            msg = ((GridIoMessage)msg).message();

            Map<Object, Short> mm = ((IgniteEx)ignite).context().discovery().discoCache().state().baselineTopology().consistentIdMapping();

            Short compactFrom = mm.get(getLocalNode().consistentId());
            Short compactTo = mm.get(node.consistentId());

            // -1 means client node.
            if (compactFrom == null)
                compactFrom = -1;

            if (compactTo == null)
                compactTo = -1;

            StringBuilder bld = new StringBuilder("SEND MSG ")
                .append("from ").append(compactFrom).append(" to ").append(compactTo).append(" ");

            Message txMsg = msg;
            ConsistentCutMarker marker = null;
            ConsistentCutMarker txMarker = null;

            if (msg instanceof ConsistentCutMarkerTxFinishMessage) {
                ConsistentCutMarkerTxFinishMessage m = (ConsistentCutMarkerTxFinishMessage)msg;

                txMsg = m.payload();
                marker = m.marker();
                txMarker = m.txMarker();
            }
            else if (msg instanceof ConsistentCutMarkerMessage) {
                ConsistentCutMarkerMessage m = (ConsistentCutMarkerMessage)msg;

                txMsg = m.payload();
                marker = m.marker();
            }

            bld
                .append(txMsg.getClass().getSimpleName())
                .append("; marker=").append(marker)
                .append("; txMarker=").append(txMarker);

            if (txMsg instanceof GridDistributedTxFinishResponse) {
                GridDistributedTxFinishResponse m = (GridDistributedTxFinishResponse)txMsg;

                bld
                    .append("; txVer=").append(m.xid().asIgniteUuid());
            }
            else if (txMsg instanceof GridDhtTxFinishRequest) {
                GridDhtTxFinishRequest m = (GridDhtTxFinishRequest)txMsg;

                bld
                    .append("; txVer=").append(m.version().asIgniteUuid());
            }
            else if (txMsg instanceof GridNearTxFinishRequest) {
                GridNearTxFinishRequest m = (GridNearTxFinishRequest)txMsg;

                bld
                    .append("; txVer=").append(m.version().asIgniteUuid());
            }
            else if (txMsg instanceof GridNearTxFinishResponse) {
                GridNearTxFinishResponse m = (GridNearTxFinishResponse)txMsg;

                bld
                    .append("; txVer=").append(m.xid().asIgniteUuid());
            }
            else if (txMsg instanceof GridDhtTxPrepareRequest) {
                GridDhtTxPrepareRequest m = (GridDhtTxPrepareRequest)txMsg;

                bld
                    .append("; txVer=").append(m.version().asIgniteUuid())
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }
            else if (txMsg instanceof GridNearTxPrepareRequest) {
                GridNearTxPrepareRequest m = (GridNearTxPrepareRequest)txMsg;

                bld
                    .append("; txVer=").append(m.version().asIgniteUuid())
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }
            else if (txMsg instanceof GridDhtTxPrepareResponse) {
                GridDhtTxPrepareResponse m = (GridDhtTxPrepareResponse)txMsg;

                bld
                    .append("; txVer=").append(m.version().asIgniteUuid());
            }
            else if (txMsg instanceof GridNearTxPrepareResponse) {
                GridNearTxPrepareResponse m = (GridNearTxPrepareResponse)txMsg;

                bld
                    .append("; txVer=").append(m.version().asIgniteUuid())
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }

            log.info(bld.toString());
        }

        /** */
        private boolean txMessage(Message msg) {
            if (msg instanceof GridIoMessage) {
                msg = ((GridIoMessage)msg).message();

                return msg.getClass().getSimpleName().contains("Tx")
                    || msg.getClass() == ConsistentCutMarkerMessage.class
                    || msg.getClass() == ConsistentCutMarkerTxFinishMessage.class;
            }

            return false;
        }
    }
}
