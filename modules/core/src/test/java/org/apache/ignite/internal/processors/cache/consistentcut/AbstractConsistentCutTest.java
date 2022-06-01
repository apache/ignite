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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/** */
public abstract class AbstractConsistentCutTest extends GridCommonAbstractTest {
    /** */
    protected static final String CACHE = "CACHE";

    /** */
    private static final int WAL_ARCHIVE_TIMEOUT = 500;

    /** */
    private static final int CONSISTENT_CUT_PERIOD = 500;

    /** */
    private final Random rnd = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalAutoArchiveAfterInactivity(WAL_ARCHIVE_TIMEOUT)
            .setPointInTimeRecoveryEnabled(true)
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

        cfg
            .setCommunicationSpi(new LogCommunicationSpi());

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
     * @return Number of nodes to run within test.
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
     * @return { nodeId -> (consistentId, compactId) }.
     */
    protected Map<Integer, T2<Serializable, Short>> stopCluster() throws Exception {
        flushWalAllGrids();

        Map<Object, Short> consistentMapping = grid(0).context().discovery().discoCache().state().baselineTopology()
            .consistentIdMapping();

        Map<Integer, T2<Serializable, Short>> m = new HashMap<>();

        for (int i = 0; i < nodes(); i++) {
            Serializable consistentId = (Serializable)grid(i).cluster().localNode().consistentId();

            short compactId = consistentMapping.get(consistentId);

            m.put(i, new T2<>(consistentId, compactId));
        }

        stopAllGrids();

        return m;
    }

    /**
     * Checks WAL from different nodes:
     * - Validate that amount of ConsistentCut records is the same.
     * - Validate that set of TX for evert CutVersion is the same.
     */
    protected void checkWals(Map<IgniteUuid, Integer> txOrigNode, int cuts, long expTxCnt) throws Exception {
        Map<Integer, T2<Serializable, Short>> top = stopCluster();

        List<ConsistentCutWalReader> states = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            log.info("Check WAL for node " + i);

            ConsistentCutWalReader reader = new ConsistentCutWalReader(i, log, txOrigNode, top);

            states.add(reader);

            reader.read();

            // Includes incomplete state also.
            assertEquals(cuts + 1, reader.cuts.size());
        }

        Map<IgniteUuid, T2<Long, Integer>> txMap = new HashMap<>();

        Set<IgniteUuid> txSet = new HashSet<>();

        for (int cutId = 0; cutId < cuts + 1; cutId++) {
            for (int nodeId = 0; nodeId < nodes(); nodeId++) {
                ConsistentCutWalReader.NodeConsistentCutState state = states.get(nodeId).cuts.get(cutId);

                for (IgniteUuid uid : state.committedTx) {
                    T2<Long, Integer> prev = txMap.put(uid, new T2<>(state.ver, nodeId));

                    if (prev != null) {
                        assertTrue("Transaction miscutted: " + uid + ". Node" + prev.get2() + "=" + prev.get1() +
                                ". Node" + nodeId + "=" + state.ver, prev.get1() == state.ver);
                    }

                    txSet.add(uid);
                }
            }
        }

        assertEquals(expTxCnt, txSet.size());
    }

    /** */
    private void flushWalAllGrids() throws Exception {
        AtomicInteger cnt = new AtomicInteger();

        multithreadedAsync(() -> {
            int idx = cnt.getAndIncrement();

            final IgniteWriteAheadLogManager walMgr = grid(idx).context().cache().context().wal();

            final long lastArchived = walMgr.lastArchivedSegment();

            int attempts = 0;

            while (walMgr.lastArchivedSegment() == lastArchived || attempts != 2 * WAL_ARCHIVE_TIMEOUT / 50) {
                try {
                    Thread.sleep(50);

                    attempts++;
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            log.info("WAL flushed for node " + idx);

        }, nodes()).get();
    }

    /** */
    protected Ignite clientNode() {
        return grid(nodes());
    }

    /** Logs TX messages between nodes. */
    protected static class LogCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (txMessage(msg))
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
                    .append("; tx=").append(m.xid().asIgniteUuid());
            }
            else if (msg instanceof GridDhtTxFinishRequest) {
                GridDhtTxFinishRequest m = (GridDhtTxFinishRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.latestCutVersion())
                    .append("; tx=").append(m.nearTxVersion().asIgniteUuid())
                    .append("; txCutVer=").append(m.txCutVersion());
            }
            else if (msg instanceof GridNearTxFinishRequest) {
                GridNearTxFinishRequest m = (GridNearTxFinishRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.latestCutVersion())
                    .append("; tx=").append(m.nearTxVersion().asIgniteUuid())
                    .append("; txCutVer=").append(m.txCutVersion());
            }
            else if (msg instanceof GridNearTxFinishResponse) {
                GridNearTxFinishResponse m = (GridNearTxFinishResponse)msg;

                bld
                    .append("; tx=").append(m.xid().asIgniteUuid());
            }
            else if (msg instanceof GridDhtTxPrepareRequest) {
                GridDhtTxPrepareRequest m = (GridDhtTxPrepareRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.latestCutVersion())
                    .append("; tx=").append((m.nearXidVersion()).asIgniteUuid())
                    .append("; dhtTx=").append((m.version()).asIgniteUuid())
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }
            else if (msg instanceof GridNearTxPrepareRequest) {
                GridNearTxPrepareRequest m = (GridNearTxPrepareRequest)msg;

                bld
                    .append("; lastCutVer=").append(m.latestCutVersion())
                    .append("; tx=").append((m.version()).asIgniteUuid())
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }
            else if (msg instanceof GridDhtTxPrepareResponse) {
                GridDhtTxPrepareResponse m = (GridDhtTxPrepareResponse)msg;

                bld
                    .append("; lastCutVer=").append(m.latestCutVersion())
                    .append("; tx=").append((m.nearTxVersion()).asIgniteUuid())
                    .append("; txCutVer=").append((m.txCutVersion()))
                    .append("; 1PC=").append((m.onePhaseCommit()));
            }
            else if (msg instanceof GridNearTxPrepareResponse) {
                GridNearTxPrepareResponse m = (GridNearTxPrepareResponse)msg;

                bld
                    .append("; lastCutVer=").append(m.latestCutVersion())
                    .append("; tx=").append((m.version().asIgniteUuid()))
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
