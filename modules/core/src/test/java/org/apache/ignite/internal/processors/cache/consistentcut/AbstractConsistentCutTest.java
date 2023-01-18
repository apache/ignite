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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
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
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
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

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** Base class for testing Consistency Cut algorithm. */
public abstract class AbstractConsistentCutTest extends GridCommonAbstractTest {
    /** */
    protected static final String CACHE = "CACHE";

    /** */
    protected static final String SNP = "base";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalCompactionEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("consistent-cut-persist")
                .setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(cacheConfiguration(CACHE));

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
            GridTestUtils.waitForCondition(() -> {
                boolean ready = true;

                for (Ignite g: G.allGrids())
                    ready &= snp((IgniteEx)g).currentCreateRequest() == null;

                return ready;
            }, 10);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Checks WALs for correct Consistency Cut.
     *
     * @param txCnt Count of run transactions.
     * @param cuts  Number of Consistent Cuts was run within a test.
     */
    protected void checkWalsConsistency(int txCnt, int cuts) throws Exception {
        List<ConsistentCutWalReader> readers = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            try (WALIterator walIter = walIter(i)) {
                ConsistentCutWalReader reader = new ConsistentCutWalReader(walIter);

                readers.add(reader);

                reader.read();

                int expCuts = reader.cuts.get(reader.cuts.size() - 1).id != null ? cuts : cuts + 1;

                assertEquals(expCuts, reader.cuts.size());
            }
        }

        // Transaction ID -> (cutId, nodeId).
        Map<GridCacheVersion, T2<UUID, Integer>> txMap = new HashMap<>();

        // Includes incomplete state also.
        for (int cutId = 0; cutId < cuts + 1; cutId++) {
            for (int nodeId = 0; nodeId < nodes(); nodeId++) {
                // Skip if the latest cut is completed.
                if (readers.get(nodeId).cuts.size() == cutId)
                    continue;

                ReadConsistentCut cut = readers.get(nodeId).cuts.get(cutId);

                for (GridCacheVersion xid: cut.txs) {
                    T2<UUID, Integer> prev = txMap.put(xid, new T2<>(cut.id, nodeId));

                    if (prev != null) {
                        assertTrue("Transaction miscutted: [xid=" + xid + ", node" + prev.get2() + "=" + prev.get1() +
                            ", node" + nodeId + "=" + cut.id,
                            Objects.equals(prev == null ? null : prev.get1(), cut.id));
                    }
                }
            }
        }

        assertEquals(txCnt, txMap.size());
    }

    /** Get iterator over WAL. */
    protected WALIterator walIter(int nodeIdx) throws Exception {
        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        String subfolderName = U.maskForFileName(getTestIgniteInstanceName(nodeIdx));

        File wal = Paths.get(workDir).resolve(DataStorageConfiguration.DFLT_WAL_PATH).resolve(subfolderName).toFile();
        File archive = Paths.get(workDir).resolve(DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH).resolve(subfolderName).toFile();

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(wal, archive);

        return factory.iterator(params);
    }

    /**
     * Read WAL and sort transactions by Consistent Cuts.
     */
    private static class ConsistentCutWalReader {
        /** Iterator over WAL archive files. */
        private final WALIterator walIter;

        /** Collection of Consistent Cuts. */
        final List<ReadConsistentCut> cuts = new ArrayList<>();

        /** For test purposes. */
        ConsistentCutWalReader(WALIterator walIter) {
            this.walIter = walIter;
        }

        /** Read WAL and fills {@link #cuts} with read Consistent Cuts. */
        void read() {
            cuts.add(new ReadConsistentCut());

            while (walIter.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> next = walIter.next();

                WALRecord rec = next.getValue();

                ReadConsistentCut cut = currentCut();

                if (rec.type() == DATA_RECORD_V2)
                    cut.addTransaction(((DataRecord)rec).writeEntries().get(0).nearXidVersion());
                else if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                    assert cut.id == null : "Lost FINISH record: " + rec;

                    cut.id = ((ConsistentCutStartRecord)rec).id();

                    cuts.add(ReadConsistentCut.fromPrev(currentCut()));
                }
                else if (rec.type() == CONSISTENT_CUT_FINISH_RECORD)
                    cut.finishCut((ConsistentCutFinishRecord)rec);
            }
        }

        /** */
        private ReadConsistentCut currentCut() {
            return cuts.get(cuts.size() - 1);
        }
    }

    /** Consistent Cut state read from WAL. */
    static class ReadConsistentCut {
        /** Previous Consistent Cut. */
        private @Nullable AbstractConsistentCutTest.ReadConsistentCut prev;

        /** Consistent Cut ID. */
        private UUID id;

        /** Set of transactions ids. */
        @GridToStringInclude
        private final Set<GridCacheVersion> txs = new HashSet<>();

        /** */
        private static ReadConsistentCut fromPrev(ReadConsistentCut prev) {
            ReadConsistentCut cut = new ReadConsistentCut();
            cut.prev = prev;

            return cut;
        }

        /** */
        private void addTransaction(GridCacheVersion txId) {
            txs.add(txId);
        }

        /** */
        private void finishCut(ConsistentCutFinishRecord rec) {
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
