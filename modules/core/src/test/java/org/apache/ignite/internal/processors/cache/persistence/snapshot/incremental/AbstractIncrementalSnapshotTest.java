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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.INCREMENTAL_SNAPSHOT_START_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** Base class for testing incremental snapshot algorithm. */
public abstract class AbstractIncrementalSnapshotTest extends GridCommonAbstractTest {
    /** */
    protected static final String CACHE = "CACHE";

    /** */
    protected static final String SNP = "base";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE));

        if (cfg.isClientMode())
            return cfg;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalCompactionEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("incremental-snapshot-persist")
                .setPersistenceEnabled(true)));

        cfg.setConsistentId(instanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);

        startClientGrid(nodes());

        snp(grid(0)).createSnapshot(SNP).get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration(String cacheName) {
        return new CacheConfiguration<Integer, Integer>()
            .setName(cacheName)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(backups());
    }

    /**
     * @return Number of server nodes.
     */
    protected abstract int nodes();

    /**
     * @return Number of backups for cache.
     */
    protected abstract int backups();

    /** */
    protected void awaitSnapshotResourcesCleaned() {
        try {
            assertTrue(GridTestUtils.waitForCondition(() -> {
                for (Ignite g: G.allGrids()) {
                    if (snp((IgniteEx)g).currentCreateRequest() != null)
                        return false;
                }

                return true;
            }, getTestTimeout(), 0));
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Checks WALs for correct incremental snapshots.
     *
     * @param txCnt Count of run transactions.
     * @param snpCnt Count of incremental snapshots were run within a test.
     */
    protected void checkWalsConsistency(int txCnt, int snpCnt) throws Exception {
        List<IncrementalSnapshotWalReader> readers = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            try (WALIterator walIter = walIter(i)) {
                IncrementalSnapshotWalReader reader = new IncrementalSnapshotWalReader(walIter);

                readers.add(reader);

                reader.read();

                int expSnpCnt = reader.snps.get(reader.snps.size() - 1).id != null ? snpCnt : snpCnt + 1;

                assertEquals(expSnpCnt, reader.snps.size());
            }
        }

        // Transaction ID -> (incSnpId, nodeIdx).
        Map<GridCacheVersion, T2<UUID, Integer>> txMap = new HashMap<>();

        // +1 - includes incomplete state also.
        for (int snpId = 0; snpId < snpCnt + 1; snpId++) {
            for (int nodeIdx = 0; nodeIdx < nodes(); nodeIdx++) {
                // Skip if the latest snapshot is completed.
                if (readers.get(nodeIdx).snps.size() == snpId)
                    continue;

                IncrementalSnapshot snp = readers.get(nodeIdx).snps.get(snpId);

                for (GridCacheVersion xid: snp.txs) {
                    T2<UUID, Integer> prev = txMap.put(xid, new T2<>(snp.id, nodeIdx));

                    if (prev != null) {
                        assertTrue("Transaction missed: [xid=" + xid + ", node" + prev.get2() + "=" + prev.get1() +
                            ", node" + nodeIdx + "=" + snp.id,
                            Objects.equals(prev == null ? null : prev.get1(), snp.id));
                    }
                }
            }
        }

        assertEquals(txCnt, txMap.size());
    }

    /** Get iterator over WAL. */
    protected WALIterator walIter(int nodeIdx) throws Exception {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        NodeFileTree ft = new NodeFileTree(U.defaultWorkDirectory(), U.maskForFileName(getTestIgniteInstanceName(nodeIdx)));

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(ft.wal(), ft.walArchive());

        return factory.iterator(params);
    }

    /** Prepare for snapshot restoring - restart grids, with clean persistence. */
    protected void restartWithCleanPersistence(int nodes, List<String> caches) throws Exception {
        stopAllGrids();

        cleanPersistenceDir(true);

        startGrids(nodes);

        grid(0).cluster().state(ClusterState.ACTIVE);

        // Caches are configured with IgniteConiguration, need to destroy them before restoring snapshot.
        grid(0).destroyCaches(caches);

        awaitPartitionMapExchange();
    }

    /** Rolls WAL segment for specified grid. */
    protected void rollWalSegment(IgniteEx g) {
        g.context().cache().context().database().checkpointReadLock();

        try {
            g.context().cache().context().wal().log(new ClusterSnapshotRecord("dummy"), RolloverType.CURRENT_SEGMENT);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
        finally {
            g.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Read WAL and sort transactions by incremental snapshots.
     */
    private static class IncrementalSnapshotWalReader {
        /** Iterator over WAL archive files. */
        private final WALIterator walIter;

        /** Collection of incremental snapshots. */
        final List<IncrementalSnapshot> snps = new ArrayList<>();

        /** For test purposes. */
        IncrementalSnapshotWalReader(WALIterator walIter) {
            this.walIter = walIter;
        }

        /** Read WAL and fills {@link #snps} with read incremental snapshot. */
        void read() {
            snps.add(new IncrementalSnapshot());

            while (walIter.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> next = walIter.next();

                WALRecord rec = next.getValue();

                IncrementalSnapshot snp = currentSnapshot();

                if (rec.type() == DATA_RECORD_V2)
                    snp.addTransaction(((DataRecord)rec).writeEntries().get(0).nearXidVersion());
                else if (rec.type() == INCREMENTAL_SNAPSHOT_START_RECORD) {
                    assert snp.id == null : "Lost FINISH record: " + rec;

                    snp.id = ((IncrementalSnapshotStartRecord)rec).id();

                    snps.add(IncrementalSnapshot.fromPrev(currentSnapshot()));
                }
                else if (rec.type() == INCREMENTAL_SNAPSHOT_FINISH_RECORD)
                    snp.finish((IncrementalSnapshotFinishRecord)rec);
            }
        }

        /** */
        private IncrementalSnapshot currentSnapshot() {
            return snps.get(snps.size() - 1);
        }
    }

    /** Incremental snapshot state read from WAL. */
    static class IncrementalSnapshot {
        /** Previous incremental snapshot. */
        private @Nullable AbstractIncrementalSnapshotTest.IncrementalSnapshot prev;

        /** Incremental snapshot ID. */
        private UUID id;

        /** Set of transactions ids. */
        @GridToStringInclude
        private final Set<GridCacheVersion> txs = new HashSet<>();

        /** */
        private static IncrementalSnapshot fromPrev(IncrementalSnapshot prev) {
            IncrementalSnapshot snp = new IncrementalSnapshot();
            snp.prev = prev;

            return snp;
        }

        /** */
        private void addTransaction(GridCacheVersion txId) {
            txs.add(txId);
        }

        /** */
        private void finish(IncrementalSnapshotFinishRecord rec) {
            assert rec.id().equals(prev.id) : prev.id + " " + rec;

            for (GridCacheVersion txId: rec.included()) {
                if (txs.remove(txId))
                    prev.txs.add(txId);
            }

            for (GridCacheVersion txId: rec.excluded()) {
                if (prev.txs.remove(txId))
                    txs.add(txId);
            }
        }
    }
}
