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

package org.apache.ignite.internal.pagemem.wal.record;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.wal.record.RecordUtils;
import org.junit.Test;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.ZIP_SUFFIX;

/**
 * Tests of serialization and deserialization of all WAL record types {@link org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType}.
 *
 * It checks that all records can be successfully deserialized from early serialized record included serialization via
 * compaction.
 */
public class WALRecordSerializationTest extends GridCommonAbstractTest {
    /** Wal segment size. */
    private static final int WAL_SEGMENT_SIZE = 4 * 1024 * 1024;

    /** **/
    private boolean compactionEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(200 * 1024 * 1024))
            .setWalSegmentSize(WAL_SEGMENT_SIZE)
            .setWalCompactionEnabled(compactionEnabled));

        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} **/
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} **/
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testAllWalRecordsSerializedAndDeserializedSuccessfully() throws Exception {
        compactionEnabled = false;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WALRecord.RecordType[] recordTypes = WALRecord.RecordType.values();

        List<ReflectionEquals> serializedRecords = new ArrayList<>();

        IgniteWriteAheadLogManager wal = ignite.context().cache().context().wal();

        ignite.context().cache().context().database().checkpointReadLock();
        try {
            for (WALRecord.RecordType recordType : recordTypes) {
                WALRecord record = RecordUtils.buildWalRecord(recordType);

                if (RecordUtils.isIncludeIntoLog(record)) {
                    serializedRecords.add(new ReflectionEquals(record, "prev", "pos",
                        "updateCounter" //updateCounter for PartitionMetaStateRecord isn't serialized.
                    ));

                    wal.log(record);
                }
            }

            wal.flush(null, true);

        }
        finally {
            ignite.context().cache().context().database().checkpointReadUnlock();
        }

        stopGrid(0);

        Iterator<ReflectionEquals> serializedIter = serializedRecords.iterator();
        ReflectionEquals curExpRecord = serializedIter.hasNext() ? serializedIter.next() : null;

        try (WALIterator iter = wal.replay(null)) {
            while (iter.hasNext()) {
                WALRecord record = iter.nextX().get2();

                if (curExpRecord != null && curExpRecord.matches(record))
                    curExpRecord = serializedIter.hasNext() ? serializedIter.next() : null;
            }
        }

        assertNull("Expected record '" + curExpRecord + "' not found.", curExpRecord);
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testAllWalRecordsSerializedCompressedAndThenDeserializedSuccessfully() throws Exception {
        compactionEnabled = true;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WALRecord.RecordType[] recordTypes = WALRecord.RecordType.values();

        List<ReflectionEquals> serializedRecords = new ArrayList<>();

        IgniteWriteAheadLogManager wal = ignite.context().cache().context().wal();

        WALPointer lastPointer = null;

        ignite.context().cache().context().database().checkpointReadLock();
        try {
            for (WALRecord.RecordType recordType : recordTypes) {
                WALRecord record = RecordUtils.buildWalRecord(recordType);

                if (RecordUtils.isIncludeIntoLog(record) && (recordType.purpose() == WALRecord.RecordPurpose.LOGICAL ||
                    recordType == WALRecord.RecordType.CHECKPOINT_RECORD)) {
                    serializedRecords.add(new ReflectionEquals(record, "prev", "pos",
                        "updateCounter" //updateCounter for PartitionMetaStateRecord isn't serialized.
                    ));

                    lastPointer = wal.log(record);
                }
            }

            wal.flush(null, true);

        }
        finally {
            ignite.context().cache().context().database().checkpointReadUnlock();
        }

        String nodeFolderName = ignite.context().pdsFolderResolver().resolveFolders().folderName();
        File nodeArchiveDir = Paths.get(
            U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false).getAbsolutePath(),
            "wal",
            "archive",
            nodeFolderName
        ).toFile();
        File walSegment = new File(nodeArchiveDir, FileDescriptor.fileName(((FileWALPointer)lastPointer).index()));
        File walZipSegment = new File(nodeArchiveDir, FileDescriptor.fileName(((FileWALPointer)lastPointer).index()) + ZIP_SUFFIX);

        // Spam WAL to move all data records to compressible WAL zone.
        for (int i = 0; i < WAL_SEGMENT_SIZE / DFLT_PAGE_SIZE * 2; i++)
            wal.log(new PageSnapshot(new FullPageId(-1, -1), new byte[DFLT_PAGE_SIZE], 1));

        ignite.getOrCreateCache("generateDirtyPages");

        // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
        ignite.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ignite.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        for (int i = 0; i < WAL_SEGMENT_SIZE / DFLT_PAGE_SIZE * 2; i++)
            wal.log(new PageSnapshot(new FullPageId(-1, -1), new byte[DFLT_PAGE_SIZE], 1));

        // Awaiting of zipping of the desirable segment.
        assertTrue(GridTestUtils.waitForCondition(walZipSegment::exists, 15_000));

        // Awaiting of removing of the desirable segment.
        assertTrue(GridTestUtils.waitForCondition(() -> !walSegment.exists(), 15_000));

        stopGrid(0);

        Iterator<ReflectionEquals> serializedIter = serializedRecords.iterator();
        ReflectionEquals curExpRecord = serializedIter.hasNext() ? serializedIter.next() : null;

        try (WALIterator iter = wal.replay(null)) {
            while (iter.hasNext()) {
                WALRecord record = iter.nextX().get2();

                if (curExpRecord != null && curExpRecord.matches(record))
                    curExpRecord = serializedIter.hasNext() ? serializedIter.next() : null;
            }
        }

        assertNull("Expected record '" + curExpRecord + "' not found.", curExpRecord);
    }
}
