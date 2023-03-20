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

import java.io.File;
import java.util.Arrays;
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.snapshotMetaFileName;
import static org.junit.Assume.assumeFalse;

/**
 * Incremental snapshots checks tests.
 */
public class IncrementalSnapshotCheckBeforeRestoreTest extends AbstractSnapshotSelfTest {
    /** */
    private static final String SNP = "snp";

    /** */
    private static final int GRID_CNT = 3;

    /** */
    private IgniteEx srv;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setWalCompactionEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        if (encryption)
            assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        srv = startGridsWithCache(
            GRID_CNT,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        startClientGrid(
            GRID_CNT,
            (UnaryOperator<IgniteConfiguration>)
                cfg -> cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
        );
    }

    /** */
    @Test
    public void testCheckCorrectIncrementalSnapshot() throws Exception {
        createFullSnapshot();

        int incSnpCnt = 3;

        createIncrementalSnapshots(incSnpCnt);

        for (IgniteEx n: F.asList(grid(0), grid(GRID_CNT))) {
            for (int i = 0; i <= incSnpCnt; i++) {
                SnapshotPartitionsVerifyTaskResult res = snp(n).checkSnapshot(SNP, null, null, false, i)
                    .get(getTestTimeout());

                assertTrue(res.exceptions().isEmpty());
                assertTrue(res.idleVerifyResult().exceptions().isEmpty());
            }
        }
    }

    /** */
    @Test
    public void testNonExistentIncrementalSnapshot() throws Exception {
        createFullSnapshot();

        for (IgniteEx n : F.asList(grid(0), grid(GRID_CNT))) {
            GridTestUtils.assertThrows(
                log,
                () -> snp(n).checkSnapshot(SNP, null, null, false, 1).get(getTestTimeout()),
                IgniteCheckedException.class,
                "No incremental snapshot found");
        }

        createIncrementalSnapshots(1);

        for (IgniteEx n : F.asList(grid(0), grid(GRID_CNT))) {
            SnapshotPartitionsVerifyTaskResult res = snp(n).checkSnapshot(SNP, null, null, false, 1)
                    .get(getTestTimeout());

            assertTrue(res.exceptions().isEmpty());
            assertTrue(res.idleVerifyResult().exceptions().isEmpty());
        }
    }

    /** */
    @Test
    public void testNoFullSnapshotMetaNotFound() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(1);

        U.delete(new File(
            snp(srv).snapshotLocalDir(SNP),
            snapshotMetaFileName((String)srv.localNode().consistentId())));

        for (IgniteEx n : F.asList(srv, grid(GRID_CNT))) {
            GridTestUtils.assertThrows(
                log,
                () -> snp(n).checkSnapshot(SNP, null, null, false, 1).get(getTestTimeout()),
                IgniteCheckedException.class,
                "Failed to find snapshot metafile");
        }
    }

    /** */
    @Test
    public void testIntermediateSnapshotNotFound() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(2);

        U.delete(snp(srv).incrementalSnapshotLocalDir(SNP, null, 1));

        for (IgniteEx n : F.asList(srv, grid(GRID_CNT))) {
            SnapshotPartitionsVerifyTaskResult res = snp(n).checkSnapshot(SNP, null, null, false, 0)
                    .get(getTestTimeout());

            assertTrue(res.exceptions().isEmpty());
            assertTrue(res.idleVerifyResult().exceptions().isEmpty());

            for (int i = 1; i <= 2; i++) {
                final int inc = i;

                GridTestUtils.assertThrows(
                    log,
                    () -> snp(n).checkSnapshot(SNP, null, null, false, inc).get(getTestTimeout()),
                    IgniteCheckedException.class,
                    "No incremental snapshot found");
            }
        }
    }

    /** */
    @Test
    public void testWalSegmentsNotFound() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(2);

        deleteWalSegment(0);

        for (IgniteEx n : F.asList(srv, grid(GRID_CNT))) {
            SnapshotPartitionsVerifyTaskResult res = snp(n).checkSnapshot(SNP, null, null, false, 0)
                    .get(getTestTimeout());

            assertTrue(res.exceptions().isEmpty());
            assertTrue(res.idleVerifyResult().exceptions().isEmpty());

            for (int i = 1; i <= 2; i++) {
                final int inc = i;

                GridTestUtils.assertThrows(
                    log,
                    () -> snp(n).checkSnapshot(SNP, null, null, false, inc).get(getTestTimeout()),
                    IgniteCheckedException.class,
                    "No WAL segments found for incremental snapshot");
            }
        }
    }

    /** */
    @Test
    public void testFirstWalSegmentNotFound() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(1, 3);

        deleteWalSegment(0);

        GridTestUtils.assertThrows(
            log,
            () -> snp(srv).checkSnapshot(SNP, null, null, false, 1).get(getTestTimeout()),
            IgniteCheckedException.class,
            "Missed WAL segment");
    }

    /** */
    @Test
    public void testLastWalSegmentNotFound() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(1, 3);

        deleteWalSegment(-1);

        GridTestUtils.assertThrows(
                log,
                () -> snp(srv).checkSnapshot(SNP, null, null, false, 1).get(getTestTimeout()),
                IgniteCheckedException.class,
                "Missed WAL segment");
    }

    /** */
    @Test
    public void testIntermediateWalSegmentNotFound() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(1, 3);

        File[] segments = incrementalSnapshotWalsDir()
            .listFiles(f -> FileWriteAheadLogManager.WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(f.getName()).matches());

        Arrays.sort(segments);

        deleteWalSegment(1);

        GridTestUtils.assertThrows(
            log,
            () -> snp(srv).checkSnapshot(SNP, null, null, false, 1).get(getTestTimeout()),
            IgniteCheckedException.class,
            "Missed WAL segments");
    }

    /** */
    @Test
    public void testNoIncrementalSnapshotMetaNotFound() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(2);

        File incMetaFile = new File(
            snp(srv).incrementalSnapshotLocalDir(SNP, null, 1),
            snapshotMetaFileName(srv.localNode().consistentId().toString()));

        IncrementalSnapshotMetadata meta = snp(srv).readFromFile(incMetaFile);

        U.delete(incMetaFile);

        snp(srv).storeSnapshotMeta(new IncrementalSnapshotMetadata(
            meta.requestId(),
            meta.snapshotName() + "1",
            meta.incrementIndex(),
            meta.consistentId(),
            null,
            meta.incrementalSnapshotPointer()), incMetaFile);

        for (IgniteEx n : F.asList(srv, grid(GRID_CNT))) {
            GridTestUtils.assertThrows(
                log,
                () -> snp(n).checkSnapshot(SNP, null, null, false, 1).get(getTestTimeout()),
                IgniteCheckedException.class,
                "Incremental snapshot doesn't match full snapshot");
        }
    }

    /** */
    @Test
    public void testIncrementIndexMismatch() throws Exception {
        createFullSnapshot();
        createIncrementalSnapshots(2);

        File incMetaFile = new File(
            snp(srv).incrementalSnapshotLocalDir(SNP, null, 1),
            snapshotMetaFileName(srv.localNode().consistentId().toString()));

        IncrementalSnapshotMetadata meta = snp(srv).readFromFile(incMetaFile);

        U.delete(incMetaFile);

        snp(srv).storeSnapshotMeta(new IncrementalSnapshotMetadata(
            meta.requestId(),
            meta.snapshotName(),
            meta.incrementIndex() + 1,
            meta.consistentId(),
            null,
            meta.incrementalSnapshotPointer()), incMetaFile);

        for (IgniteEx n : F.asList(srv, grid(GRID_CNT))) {
            GridTestUtils.assertThrows(
                log,
                () -> snp(n).checkSnapshot(SNP, null, null, false, 1).get(getTestTimeout()),
                IgniteCheckedException.class,
                "Incremental snapshot meta has wrong index");
        }
    }

    /** */
    private void createFullSnapshot() {
        snp(grid(0)).createSnapshot(SNP, onlyPrimary).get(TIMEOUT);
    }

    /** */
    private void createIncrementalSnapshots(int incSnpCnt) throws Exception {
        createIncrementalSnapshots(incSnpCnt, 1);
    }

    /** */
    private void createIncrementalSnapshots(int incSnpCnt, int walSegCnt) throws Exception {
        for (int i = 0; i < incSnpCnt; i++) {
            for (int j = 0; j < walSegCnt; j++) {
                loadDataToPartition(i, srv.name(), DEFAULT_CACHE_NAME, 1000, 0);

                // Dummy roll of WAL.
                if (walSegCnt > 1)
                    forceRollWal();
            }

            snp(grid(0)).createIncrementalSnapshot(SNP).get(TIMEOUT);
        }
    }

    /** */
    private void deleteWalSegment(int idx) {
        File[] segments = incrementalSnapshotWalsDir()
            .listFiles(f -> FileWriteAheadLogManager.WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(f.getName()).matches());

        Arrays.sort(segments);

        // Last segment.
        if (idx == -1)
            idx = segments.length - 1;

        U.delete(segments[idx]);
    }

    /** */
    private void forceRollWal() throws Exception {
        srv.context().cache().context().database().checkpointReadLock();

        try {
            srv.context().cache().context().wal().log(new ClusterSnapshotRecord(""), RolloverType.CURRENT_SEGMENT);
        }
        finally {
            srv.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    private File incrementalSnapshotWalsDir() {
        return IgniteSnapshotManager.incrementalSnapshotWalsDir(
            snp(srv).incrementalSnapshotLocalDir(SNP, null, 1),
            srv.localNode().consistentId().toString());
    }
}
