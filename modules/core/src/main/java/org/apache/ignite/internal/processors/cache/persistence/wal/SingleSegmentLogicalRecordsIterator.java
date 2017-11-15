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
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.RecordTypes;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.util.typedef.P2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates over logical records of one WAL segment from archive. Used for WAL archive compression.
 */
public class SingleSegmentLogicalRecordsIterator extends AbstractWalRecordsIterator {
    /** Segment initialized flag. */
    private boolean segmentInitialized;

    /** Archived segment index. */
    private long archivedSegIdx;

    /** Archive directory. */
    private File archiveDir;
    /**
     * @param log Logger.
     * @param sharedCtx Shared context.
     * @param ioFactory Io factory.
     * @param bufSize Buffer size.
     * @param archivedSegIdx Archived seg index.
     * @param archiveDir Directory with segment.
     */
    public SingleSegmentLogicalRecordsIterator(
        @NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext sharedCtx,
        @NotNull FileIOFactory ioFactory,
        int bufSize,
        long archivedSegIdx,
        File archiveDir
    ) throws IgniteCheckedException {
        super(log, sharedCtx, initLogicalRecordsSerializerFactory(sharedCtx), ioFactory, bufSize);

        this.archivedSegIdx = archivedSegIdx;
        this.archiveDir = archiveDir;

        advance();
    }

    /**
     * @param sharedCtx Shared context.
     */
    private static RecordSerializerFactory initLogicalRecordsSerializerFactory(GridCacheSharedContext sharedCtx)
        throws IgniteCheckedException {

        return new RecordSerializerFactoryImpl(sharedCtx)
            .recordDeserializeFilter(new LogicalRecordsFilter())
            .marshalledMode(true);
    }

    /** {@inheritDoc} */
    @Override protected FileWriteAheadLogManager.ReadFileHandle advanceSegment(
        @Nullable FileWriteAheadLogManager.ReadFileHandle curWalSegment) throws IgniteCheckedException {
        if (segmentInitialized) {
            closeCurrentWalSegment();
            // No advance as we iterate over single segment.
            return null;
        }
        else {
            segmentInitialized = true;

            FileWriteAheadLogManager.FileDescriptor fd = new FileWriteAheadLogManager.FileDescriptor(
                new File(archiveDir, FileWriteAheadLogManager.FileDescriptor.fileName(archivedSegIdx)));

            try {
                return initReadHandle(fd, null);
            }
            catch (FileNotFoundException e) {
                throw new IgniteCheckedException("Missing WAL segment in the archive", e);
            }
        }
    }

    /**
     *
     */
    private static class LogicalRecordsFilter implements P2<WALRecord.RecordType, WALPointer> {
        /** Records type to skip. */
        private final Set<WALRecord.RecordType> skip = RecordTypes.DELTA_TYPE_SET;

        /** {@inheritDoc} */
        @Override public boolean apply(WALRecord.RecordType type, WALPointer ptr) {
            return !skip.contains(type);
        }
    }
}
