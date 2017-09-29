/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterator over WAL segments. This abstract class provides most functionality for reading records in log.
 * Subclasses are to override segment switching functionality
 */
public abstract class AbstractWalRecordsIterator
    extends GridCloseableIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>> implements WALIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Current record preloaded, to be returned on next()<br>
     * Normally this should be not null because advance() method should already prepare some value<br>
     */
    protected IgniteBiTuple<WALPointer, WALRecord> curRec;

    /**
     * Current WAL segment absolute index. <br>
     * Determined as lowest number of file at start, is changed during advance segment
     */
    protected long curWalSegmIdx = -1;

    /**
     * Current WAL segment read file handle. To be filled by subclass advanceSegment
     */
    private FileWriteAheadLogManager.ReadFileHandle currWalSegment;

    /** Logger */
    @NotNull protected final IgniteLogger log;

    /**
     * Shared context for creating serializer of required version and grid name access. Also cacheObjects processor from
     * this context may be used to covert Data entry key and value from its binary representation into objects.
     */
    @NotNull protected final GridCacheSharedContext sharedCtx;

    /** Serializer of current version to read headers. */
    @NotNull private final RecordSerializer serializer;

    /** Factory to provide I/O interfaces for read/write operations with files */
    @NotNull protected final FileIOFactory ioFactory;

    /** Utility buffer for reading records */
    private final ByteBufferExpander buf;

    /**
     * @param log Logger.
     * @param sharedCtx Shared context.
     * @param serializer Serializer of current version to read headers.
     * @param ioFactory ioFactory for file IO access.
     * @param bufSize buffer for reading records size.
     */
    protected AbstractWalRecordsIterator(
        @NotNull final IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx,
        @NotNull final RecordSerializer serializer,
        @NotNull final FileIOFactory ioFactory,
        final int bufSize
    ) {
        this.log = log;
        this.sharedCtx = sharedCtx;
        this.serializer = serializer;
        this.ioFactory = ioFactory;

        buf = new ByteBufferExpander(bufSize, ByteOrder.nativeOrder());
    }

    /**
     * Scans provided folder for a WAL segment files
     * @param walFilesDir directory to scan
     * @return found WAL file descriptors
     */
    protected static FileWriteAheadLogManager.FileDescriptor[] loadFileDescriptors(@NotNull final File walFilesDir) throws IgniteCheckedException {
        final File[] files = walFilesDir.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        if (files == null) {
            throw new IgniteCheckedException("WAL files directory does not not denote a " +
                "directory, or if an I/O error occurs: [" + walFilesDir.getAbsolutePath() + "]");
        }
        return FileWriteAheadLogManager.scan(files);
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<WALPointer, WALRecord> onNext() throws IgniteCheckedException {
        IgniteBiTuple<WALPointer, WALRecord> ret = curRec;

        advance();

        return ret;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return curRec != null;
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IgniteCheckedException {
        try {
            buf.close();
        }
        catch (Exception ex) {
            throw new IgniteCheckedException(ex);
        }
    }

    /**
     * Switches records iterator to the next record.
     * <ul>
     * <li>{@link #curRec} will be updated.</li>
     * <li> If end of segment reached, switch to new segment is called. {@link #currWalSegment} will be updated.</li>
     * </ul>
     *
     * {@code advance()} runs a step ahead {@link #next()}
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void advance() throws IgniteCheckedException {
        while (true) {
            try {
                curRec = advanceRecord(currWalSegment);

                if (curRec != null)
                    return;
                else {
                    currWalSegment = advanceSegment(currWalSegment);

                    if (currWalSegment == null)
                        return;
                }
            }
            catch (WalSegmentTailReachedException e) {
                log.warning(e.getMessage());

                curRec = null;

                return;
            }
        }
    }

    /**
     * Closes and returns WAL segment (if any)
     *
     * @return closed handle
     * @throws IgniteCheckedException if IO failed
     */
    @Nullable protected FileWriteAheadLogManager.ReadFileHandle closeCurrentWalSegment() throws IgniteCheckedException {
        final FileWriteAheadLogManager.ReadFileHandle walSegmentClosed = currWalSegment;

        if (walSegmentClosed != null) {
            walSegmentClosed.close();
            currWalSegment = null;
        }
        return walSegmentClosed;
    }

    /**
     * Switches records iterator to the next WAL segment
     * as result of this method, new reference to segment should be returned.
     * Null for current handle means stop of iteration
     * @throws IgniteCheckedException if reading failed
     * @param curWalSegment current open WAL segment or null if there is no open segment yet
     * @return new WAL segment to read or null for stop iteration
     */
    protected abstract FileWriteAheadLogManager.ReadFileHandle advanceSegment(
        @Nullable final FileWriteAheadLogManager.ReadFileHandle curWalSegment) throws IgniteCheckedException;

    /**
     * Switches to new record
     * @param hnd currently opened read handle
     * @return next advanced record
     */
    private IgniteBiTuple<WALPointer, WALRecord> advanceRecord(
        @Nullable final FileWriteAheadLogManager.ReadFileHandle hnd
    ) throws IgniteCheckedException {
        if (hnd == null)
            return null;

        final FileWALPointer ptr = new FileWALPointer(
            hnd.idx,
            (int)hnd.in.position(),
            0);

        try {
            final WALRecord rec = hnd.ser.readRecord(hnd.in, ptr);

            ptr.length(rec.size());

            // cast using diamond operator here can break compile for 7
            return new IgniteBiTuple<>((WALPointer)ptr, postProcessRecord(rec));
        }
        catch (IOException | IgniteCheckedException e) {
            if (e instanceof WalSegmentTailReachedException)
                throw (WalSegmentTailReachedException)e;

            if (!(e instanceof SegmentEofException))
                handleRecordException(e, ptr);

            return null;
        }
    }

    /**
     * Performs final conversions with record loaded from WAL.
     * To be overridden by subclasses if any processing required.
     *
     * @param rec record to post process.
     * @return post processed record.
     */
    @NotNull protected WALRecord postProcessRecord(@NotNull final WALRecord rec) {
        return rec;
    }

    /**
     * Handler for record deserialization exception
     * @param e problem from records reading
     * @param ptr file pointer was accessed
     */
    protected void handleRecordException(
        @NotNull final Exception e,
        @Nullable final FileWALPointer ptr) {
        if (log.isInfoEnabled())
            log.info("Stopping WAL iteration due to an exception: " + e.getMessage());
    }

    /**
     * @param desc File descriptor.
     * @param start Optional start pointer. Null means read from the beginning
     * @return Initialized file handle.
     * @throws FileNotFoundException If segment file is missing.
     * @throws IgniteCheckedException If initialized failed due to another unexpected error.
     */
    protected FileWriteAheadLogManager.ReadFileHandle initReadHandle(
        @NotNull final FileWriteAheadLogManager.FileDescriptor desc,
        @Nullable final FileWALPointer start)
        throws IgniteCheckedException, FileNotFoundException {
        try {
            FileIO fileIO = ioFactory.create(desc.file);

            try {
                int serVer = FileWriteAheadLogManager.readSerializerVersion(fileIO);

                RecordSerializer ser = FileWriteAheadLogManager.forVersion(sharedCtx, serVer);

                FileInput in = new FileInput(fileIO, buf);

                if (start != null && desc.idx == start.index()) {
                    // Make sure we skip header with serializer version.
                    long startOffset = Math.max(start.fileOffset(), fileIO.position());

                    in.seek(startOffset);
                }

                return new FileWriteAheadLogManager.ReadFileHandle(fileIO, desc.idx, sharedCtx.igniteInstanceName(), ser, in);
            }
            catch (SegmentEofException | EOFException ignore) {
                try {
                    fileIO.close();
                }
                catch (IOException ce) {
                    throw new IgniteCheckedException(ce);
                }

                return null;
            }
            catch (IOException | IgniteCheckedException e) {
                try {
                    fileIO.close();
                }
                catch (IOException ce) {
                    e.addSuppressed(ce);
                }

                throw e;
            }
        }
        catch (FileNotFoundException e) {
            throw e;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(
                "Failed to initialize WAL segment: " + desc.file.getAbsolutePath(), e);
        }
    }

}
