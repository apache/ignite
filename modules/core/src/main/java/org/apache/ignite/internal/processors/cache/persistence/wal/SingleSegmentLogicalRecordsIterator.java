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
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.RecordTypes;
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
        super(log, sharedCtx, initLogicalRecordsSerializerFactory(sharedCtx), ioFactory, bufSize);

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
    @Override protected AbstractReadFileHandle createReadFileHandle(FileIO fileIO, long idx,
        RecordSerializer ser, FileInput in) {
        return new FileWriteAheadLogManager.ReadFileHandle(fileIO, idx, ser, in);
    }

    /**
     *
     */
    private static class LogicalRecordsFilter implements P2<WALRecord.RecordType, WALPointer> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Records type to skip. */
        private final Set<WALRecord.RecordType> skip = RecordTypes.DELTA_TYPE_SET;

        /** {@inheritDoc} */
        @Override public boolean apply(WALRecord.RecordType type, WALPointer ptr) {
            return !skip.contains(type);
        }
    }
}
