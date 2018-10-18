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

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
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
    private final ThreadLocal<WALPointer> lastWALPtr;

    /** */
    protected final RecordSerializer serializer;

    private final Supplier<FileWriteHandle> currentHandle;

    /** WAL segment size in bytes. . This is maximum value, actual segments may be shorter. */
    private final long maxWalSegmentSize;

    /** Fsync delay. */
    private final long fsyncDelay;

    /** Thread local byte buffer size, see {@link #tlb} */
    private final int tlbSize;

    public FsyncFileHandleManagerImpl(GridCacheSharedContext cctx,
        DataStorageMetricsImpl metrics, ThreadLocal<WALPointer> ptr, RecordSerializer serializer,
        Supplier<FileWriteHandle> handle, WALMode mode,
        long maxWalSegmentSize, long fsyncDelay, int tlbSize) {
        this.cctx = cctx;
        this.log = cctx.logger(FsyncFileHandleManagerImpl.class);
        this.mode = mode;
        this.metrics = metrics;
        lastWALPtr = ptr;
        this.serializer = serializer;
        currentHandle = handle;
        this.maxWalSegmentSize = maxWalSegmentSize;
        this.fsyncDelay = fsyncDelay;
        this.tlbSize = tlbSize;
    }

    @Override public FileWriteHandle build(SegmentIO fileIO, long position, boolean resume,
        RecordSerializer serializer) throws IOException {
        return new FsyncFileWriteHandle(            fileIO,
            position,
            maxWalSegmentSize,
            serializer, mode, tlbSize, cctx, metrics, fsyncDelay);
    }

    @Override public FileWriteHandle next(SegmentIO fileIO, long position, boolean resume,
        RecordSerializer serializer) throws IOException {
        return new FsyncFileWriteHandle(
            fileIO,
            position,
            maxWalSegmentSize,
            serializer, mode, tlbSize, cctx, metrics, fsyncDelay);
    }

    private FsyncFileWriteHandle currentHandle() {
        return (FsyncFileWriteHandle)currentHandle.get();
    }

    @Override public void start() {
        //NOOP
    }

    @Override public void stop() throws IgniteCheckedException {
        FsyncFileWriteHandle currHnd = currentHandle();

        if (mode == WALMode.BACKGROUND) {
            if (currHnd != null)
                currHnd.flushAllStop();
        }

        if (currHnd != null)
            currHnd.close(false);
    }

    @Override public void resumeLogging() {
        //NOOP
    }

    @Override public void flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return;

        FsyncFileWriteHandle cur = currentHandle();

        // WAL manager was not started (client node).
        if (cur == null)
            return;

        FileWALPointer filePtr = (FileWALPointer)(ptr == null ? lastWALPtr.get() : ptr);

        // No need to sync if was rolled over.
        if (filePtr != null && !cur.needFsync(filePtr))
            return;

        cur.fsync(filePtr, false);
    }
}
