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

package org.apache.ignite.internal.processors.cache.persistence.wal.filehandle;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;

/**
 * Implementation of {@link FileWriteHandle} for FSYNC mode.
 */
public class FsyncFileHandleManagerImpl implements FileHandleManager {
    /** Context. */
    protected final GridCacheSharedContext cctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** */
    private final WALMode mode;

    /** Persistence metrics tracker. */
    private final DataStorageMetricsImpl metrics;

    /** */
    protected final RecordSerializer serializer;

    /** Current handle supplier. */
    private final Supplier<FileWriteHandle> currentHandleSupplier;

    /** WAL segment size in bytes. This is maximum value, actual segments may be shorter. */
    private final long maxWalSegmentSize;

    /** Fsync delay. */
    private final long fsyncDelay;

    /** Thread local byte buffer size. */
    private final int tlbSize;

    /**
     * @param cctx Context.
     * @param metrics Data storage metrics.
     * @param serializer Serializer.
     * @param handle Current handle supplier.
     * @param mode WAL mode.
     * @param maxWalSegmentSize Max WAL segment size.
     * @param fsyncDelay Fsync delay.
     * @param tlbSize Thread local byte buffer size.
     */
    public FsyncFileHandleManagerImpl(
        GridCacheSharedContext cctx,
        DataStorageMetricsImpl metrics,
        RecordSerializer serializer,
        Supplier<FileWriteHandle> handle,
        WALMode mode,
        long maxWalSegmentSize,
        long fsyncDelay,
        int tlbSize
    ) {
        this.cctx = cctx;
        this.log = cctx.logger(FsyncFileHandleManagerImpl.class);
        this.mode = mode;
        this.metrics = metrics;
        this.serializer = serializer;
        currentHandleSupplier = handle;
        this.maxWalSegmentSize = maxWalSegmentSize;
        this.fsyncDelay = fsyncDelay;
        this.tlbSize = tlbSize;
    }

    /** {@inheritDoc} */
    @Override public FileWriteHandle initHandle(SegmentIO fileIO, long position,
        RecordSerializer serializer) throws IOException {
        return new FsyncFileWriteHandle(
            cctx, fileIO, metrics, serializer, position,
            mode, maxWalSegmentSize, tlbSize, fsyncDelay
        );
    }

    /** {@inheritDoc} */
    @Override public FileWriteHandle nextHandle(SegmentIO fileIO,
        RecordSerializer serializer) throws IOException {
        return new FsyncFileWriteHandle(
            cctx, fileIO, metrics, serializer, 0,
            mode, maxWalSegmentSize, tlbSize, fsyncDelay
        );
    }

    /**
     * @return Current handle.
     */
    private FsyncFileWriteHandle currentHandle() {
        return (FsyncFileWriteHandle)currentHandleSupplier.get();
    }

    /** {@inheritDoc} */
    @Override public void onDeactivate() throws IgniteCheckedException {
        FsyncFileWriteHandle currHnd = currentHandle();

        if (mode == WALMode.BACKGROUND) {
            if (currHnd != null)
                currHnd.flushAllOnStop();
        }

        if (currHnd != null)
            currHnd.close(false);
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging() {
        //NOOP.
    }

    /** {@inheritDoc} */
    @Override public WALPointer flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return null;

        FsyncFileWriteHandle cur = currentHandle();

        // WAL manager was not started (client node).
        if (cur == null)
            return null;

        FileWALPointer filePtr;

        if (ptr == null) {
            WALRecord rec = cur.head.get();

            if (rec instanceof FsyncFileWriteHandle.FakeRecord)
                return null;

            filePtr = (FileWALPointer)rec.position();
        }
        else
            filePtr = (FileWALPointer)ptr;

        // No need to sync if was rolled over.
        if (!cur.needFsync(filePtr))
            return filePtr;

        cur.fsync(filePtr, false);

        return filePtr;
    }
}
