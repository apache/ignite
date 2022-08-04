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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutTest.TestConsistentCutManager.cutMgr;
import static org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutWalReader.NodeConsistentCutState.INCOMPLETE;

/** Base class for testing Consistency Cut algorithm. */
public abstract class AbstractConsistentCutTest extends GridCommonAbstractTest {
    /** */
    protected static final String CACHE = "CACHE";

    /** */
    private static final int CONSISTENT_CUT_PERIOD = 500;

    /** */
    private final Random rnd = new Random();

    /** nodeIdx -> Consistent Node ID. */
    private final List<UUID> consistentIds = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setPitrEnabled(true)
            .setPitrPeriod(CONSISTENT_CUT_PERIOD)
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

        cfg.setConsistentId(UUID.randomUUID());

        consistentIds.add((UUID)cfg.getConsistentId());

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
     * Await that every node received Consistent Cut version.
     *
     * @param cutVer Consistent Cut version.
     */
    protected void awaitGlobalCutVersionReceived(long cutVer, int exclNodeId) throws Exception {
        awaitGlobalCutEvent((ign) ->
            cutMgr(ign).cutVersion().version() == cutVer, "CutVersionReceived " + cutVer, exclNodeId);
    }

    /**
     * Await global Consistent Cut has completed, and Ignite is ready for new Consistent Cut.
     *
     * @param expCutVer Expected Consistent Cut version.
     * @param strict If {@code true} then await exactly specified version, otherwise it finishes after getting version
     *               greater than specified.
     * @return Actual version of the latest finished Consistent Cut.
     */
    protected long awaitGlobalCutReady(long expCutVer, boolean strict) throws Exception {
        AtomicLong ver = new AtomicLong();

        // Await all nodes finishes locally.
        awaitGlobalCutEvent(ign -> {
            ConsistentCutState cutState = cutMgr(ign).currCutState();

            long cutVer = cutState.version().version();

            if (cutVer >= expCutVer) {
                boolean ready = cutState.cut() == null;

                if (ready) {
                    if (strict && cutVer != expCutVer)
                        return false;

                    ver.set(cutVer);
                }

                return ready;
            }

            return false;
        }, "GlobalCutReady " + expCutVer + " " + strict, -1);

        // Await all nodes sent finish responses and coordinator received all of them.
        GridTestUtils.waitForCondition(() ->
            cutMgr(grid(0)).consistentCutFinished(expCutVer), 60_000, 10);

        return ver.get();
    }

    /**
     * Await global Consistent Cut event has finished.
     */
    private void awaitGlobalCutEvent(
        Function<IgniteEx, Boolean> cutEvtPredicate,
        String evt,
        int exclNodeId
    ) throws Exception {
        boolean rdy = GridTestUtils.waitForCondition(() ->
            G.allGrids().stream()
                .filter(ign -> {
                    if (exclNodeId >= 0)
                        return !ign.cluster().localNode().consistentId().equals(consistentIds.get(exclNodeId));

                    return true;
                })
                .allMatch(ign -> cutEvtPredicate.apply((IgniteEx)ign)), 60_000, 10);

        if (rdy)
            return;

        StringBuilder bld = new StringBuilder()
            .append("Failed to wait Consitent Cut event: " + evt);

        bld.append("\n\tCoordinator node0 = ").append(U.isLocalNodeCoordinator(grid(0).context().discovery()));

        for (int i = 0; i < nodes(); i++)
            bld.append("\n\tNode").append(i).append( ": ").append(cutMgr(grid(i)).cutVersion());

        throw new Exception(bld.toString());
    }

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
            awaitGlobalCutReady(i, true);

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

            int expCuts = reader.cuts.get(reader.cuts.size() - 1).ver == INCOMPLETE ? cuts + 1 : cuts;

            assertEquals(expCuts, reader.cuts.size());
        }

        Map<IgniteUuid, T2<Long, Integer>> txMap = new HashMap<>();

        Map<IgniteUuid, TransactionState> txStates = new HashMap<>();

        // Includes incomplete state also.
        for (int cutId = 0; cutId < cuts + 1; cutId++) {
            for (int nodeId = 0; nodeId < nodes(); nodeId++) {
                // Skip if the latest cut wasn't INCOMPLETE.
                if (states.get(nodeId).cuts.size() == cutId)
                    continue;

                ConsistentCutWalReader.NodeConsistentCutState state = states.get(nodeId).cuts.get(cutId);

                for (IgniteUuid uid: state.committedTx) {
                    T2<Long, Integer> prev = txMap.put(uid, new T2<>(state.ver, nodeId));

                    if (prev != null) {
                        assertTrue("Transaction miscutted: " + uid + ". Node" + prev.get2() + "=" + prev.get1() +
                            ". Node" + nodeId + "=" + state.ver, prev.get1() == state.ver);
                    }

                    TransactionState prevState = txStates.put(uid, TransactionState.COMMITTED);

                    assertTrue(prevState == null || prevState == TransactionState.COMMITTED);
                }
            }
        }

        assertEquals(txNearNode.size(), txStates.size());
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
        boolean consistentCutFinished(long cutVer) {
            return U.isLocalNodeCoordinator(cctx.discovery())
                && cutVersion().version() == cutVer
                && notFinishedSrvNodes == null;
        }

        /** */
        public ConsistentCutState currCutState() {
            return CONSITENT_CUT_STATE.get(this);
        }

        /** */
        static boolean disabled(IgniteEx ign) {
            return cutMgr(ign).scheduleConsistentCutTask == null;
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
                .append("from ").append(compactFrom).append(" to ").append(compactTo).append(" ")
                .append(msg.getClass().getSimpleName());

            if (msg instanceof GridDistributedTxFinishResponse) {
                GridDistributedTxFinishResponse m = (GridDistributedTxFinishResponse)msg;

                bld
                    .append("; txVer=").append(m.xid().asIgniteUuid());
            }
            else if (msg instanceof GridDhtTxFinishRequest) {
                GridDhtTxFinishRequest m = (GridDhtTxFinishRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.cutVersion())
                    .append("; txVer=").append(m.version().asIgniteUuid())
                    .append("; txCutVer=").append(m.txCutVersion());
            }
            else if (msg instanceof GridNearTxFinishRequest) {
                GridNearTxFinishRequest m = (GridNearTxFinishRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.cutVersion())
                    .append("; txVer=").append(m.version().asIgniteUuid())
                    .append("; txCutVer=").append(m.txCutVersion());
            }
            else if (msg instanceof GridNearTxFinishResponse) {
                GridNearTxFinishResponse m = (GridNearTxFinishResponse)msg;

                bld
                    .append("; txVer=").append(m.xid().asIgniteUuid());
            }
            else if (msg instanceof GridDhtTxPrepareRequest) {
                GridDhtTxPrepareRequest m = (GridDhtTxPrepareRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.cutVersion())
                    .append("; ver=").append(m.version().asIgniteUuid())
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }
            else if (msg instanceof GridNearTxPrepareRequest) {
                GridNearTxPrepareRequest m = (GridNearTxPrepareRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.cutVersion())
                    .append("; ver=").append(m.version().asIgniteUuid())
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }
            else if (msg instanceof GridDhtTxPrepareResponse) {
                GridDhtTxPrepareResponse m = (GridDhtTxPrepareResponse)msg;

                bld
                    .append("; lastCutVer=").append(m.cutVersion())
                    .append("; txVer=").append(m.version().asIgniteUuid())
                    .append("; txCutVer=").append((m.txCutVersion()));
            }
            else if (msg instanceof GridNearTxPrepareResponse) {
                GridNearTxPrepareResponse m = (GridNearTxPrepareResponse)msg;

                bld
                    .append("; lastCutVer=").append(m.cutVersion())
                    .append("; txVer=").append(m.version().asIgniteUuid())
                    .append("; txCutVer=").append((m.txCutVersion()))
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }

            log.info(bld.toString());
        }

        /** */
        private boolean txMessage(Message msg) {
            if (msg instanceof GridIoMessage) {
                msg = ((GridIoMessage)msg).message();

                return msg.getClass().getSimpleName().contains("Tx");
            }

            return false;
        }
    }
}
