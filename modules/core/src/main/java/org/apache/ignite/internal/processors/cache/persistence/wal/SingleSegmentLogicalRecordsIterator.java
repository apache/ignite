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
package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SimpleSegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.P2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates over logical records of one WAL segment from archive. Used for WAL archive compression.
 * Doesn't deserialize actual record data, returns {@link MarshalledRecord} instances instead.
 */
public class SingleSegmentLogicalRecordsIterator extends AbstractWalRecordsIterator {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Segment initialized flag. */
    private boolean segmentInitialized;

    /** Archive directory. */
    private File archiveDir;

    /** Closure which is executed right after advance. */
    private CIX1<WALRecord> advanceC;

    /**
     * @param log Logger.
     * @param sharedCtx Shared context.
     * @param ioFactory Io factory.
     * @param bufSize Buffer size.
     * @param archivedSegIdx Archived seg index.
     * @param archiveDir Directory with segment.
     * @param advanceC Closure which is executed right after advance.
     */
    SingleSegmentLogicalRecordsIterator(
        @NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext sharedCtx,
        @NotNull FileIOFactory ioFactory,
        int bufSize,
        long archivedSegIdx,
        File archiveDir,
        CIX1<WALRecord> advanceC
    ) throws IgniteCheckedException {
        super(log, sharedCtx, initLogicalRecordsSerializerFactory(sharedCtx), ioFactory, bufSize, new SimpleSegmentFileInputFactory());

        curWalSegmIdx = archivedSegIdx;
        this.archiveDir = archiveDir;
        this.advanceC = advanceC;

        advance();
    }

    /**
     * @param sharedCtx Shared context.
     */
    private static RecordSerializerFactory initLogicalRecordsSerializerFactory(GridCacheSharedContext sharedCtx)
        throws IgniteCheckedException {

        return new RecordSerializerFactoryImpl(sharedCtx, new LogicalRecordsFilter()).marshalledMode(true);
    }

    /** {@inheritDoc} */
    @Override protected AbstractReadFileHandle advanceSegment(
        @Nullable AbstractReadFileHandle curWalSegment) throws IgniteCheckedException {
        if (segmentInitialized) {
            closeCurrentWalSegment();
            // No advance as we iterate over single segment.
            return null;
        }
        else {
            segmentInitialized = true;

            FileDescriptor fd = new FileDescriptor(
                new File(archiveDir, FileDescriptor.fileName(curWalSegmIdx)));

            try {
                return initReadHandle(fd, null);
            }
            catch (FileNotFoundException e) {
                throw new IgniteCheckedException("Missing WAL segment in the archive", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void advance() throws IgniteCheckedException {
        super.advance();

        if (curRec != null && advanceC != null)
            advanceC.apply(curRec.get2());
    }

    /** {@inheritDoc} */
    @Override protected AbstractReadFileHandle createReadFileHandle(
        SegmentIO fileIO,
        RecordSerializer ser, FileInput in) {
        return new FileWriteAheadLogManager.ReadFileHandle(fileIO, ser, in, null);
    }

    /**
     *
     */
    private static class LogicalRecordsFilter implements P2<WALRecord.RecordType, WALPointer> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Set for handling WAL record types. */
        public static final Set<WALRecord.RecordType> skip = new HashSet<>();

        /**
         *  Fills up set with RecordType-s, which have to be skipped.
         */
        static {
            skip.add(WALRecord.RecordType.PAGE_RECORD);
            skip.add(WALRecord.RecordType.INIT_NEW_PAGE_RECORD);
            skip.add(WALRecord.RecordType.DATA_PAGE_INSERT_RECORD);
            skip.add(WALRecord.RecordType.DATA_PAGE_INSERT_FRAGMENT_RECORD);
            skip.add(WALRecord.RecordType.DATA_PAGE_REMOVE_RECORD);
            skip.add(WALRecord.RecordType.DATA_PAGE_SET_FREE_LIST_PAGE);
            skip.add(WALRecord.RecordType.MVCC_DATA_PAGE_MARK_UPDATED_RECORD);
            skip.add(WALRecord.RecordType.MVCC_DATA_PAGE_TX_STATE_HINT_UPDATED_RECORD);
            skip.add(WALRecord.RecordType.MVCC_DATA_PAGE_NEW_TX_STATE_HINT_UPDATED_RECORD);
            skip.add(WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT);
            skip.add(WALRecord.RecordType.BTREE_META_PAGE_ADD_ROOT);
            skip.add(WALRecord.RecordType.BTREE_META_PAGE_CUT_ROOT);
            skip.add(WALRecord.RecordType.BTREE_INIT_NEW_ROOT);
            skip.add(WALRecord.RecordType.BTREE_PAGE_RECYCLE);
            skip.add(WALRecord.RecordType.BTREE_PAGE_INSERT);
            skip.add(WALRecord.RecordType.BTREE_FIX_LEFTMOST_CHILD);
            skip.add(WALRecord.RecordType.BTREE_FIX_COUNT);
            skip.add(WALRecord.RecordType.BTREE_PAGE_REPLACE);
            skip.add(WALRecord.RecordType.BTREE_PAGE_REMOVE);
            skip.add(WALRecord.RecordType.BTREE_PAGE_INNER_REPLACE);
            skip.add(WALRecord.RecordType.BTREE_FIX_REMOVE_ID);
            skip.add(WALRecord.RecordType.BTREE_FORWARD_PAGE_SPLIT);
            skip.add(WALRecord.RecordType.BTREE_EXISTING_PAGE_SPLIT);
            skip.add(WALRecord.RecordType.BTREE_PAGE_MERGE);
            skip.add(WALRecord.RecordType.PAGES_LIST_SET_NEXT);
            skip.add(WALRecord.RecordType.PAGES_LIST_SET_PREVIOUS);
            skip.add(WALRecord.RecordType.PAGES_LIST_INIT_NEW_PAGE);
            skip.add(WALRecord.RecordType.PAGES_LIST_ADD_PAGE);
            skip.add(WALRecord.RecordType.PAGES_LIST_REMOVE_PAGE);
            skip.add(WALRecord.RecordType.META_PAGE_INIT);
            skip.add(WALRecord.RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS);
            skip.add(WALRecord.RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS_V2);
            skip.add(WALRecord.RecordType.TRACKING_PAGE_DELTA);
            skip.add(WALRecord.RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID);
            skip.add(WALRecord.RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID);
            skip.add(WALRecord.RecordType.META_PAGE_UPDATE_NEXT_SNAPSHOT_ID);
            skip.add(WALRecord.RecordType.META_PAGE_UPDATE_LAST_ALLOCATED_INDEX);
            skip.add(WALRecord.RecordType.PAGE_LIST_META_RESET_COUNT_RECORD);
            skip.add(WALRecord.RecordType.DATA_PAGE_UPDATE_RECORD);
            skip.add(WALRecord.RecordType.BTREE_META_PAGE_INIT_ROOT2);
            skip.add(WALRecord.RecordType.ROTATED_ID_PART_RECORD);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(WALRecord.RecordType type, WALPointer ptr) {
            return !skip.contains(type);
        }
    }
}
