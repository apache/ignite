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
import org.apache.ignite.internal.processors.cache.persistence.file.UnzipFileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.SegmentHeader;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readSegmentHeader;

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
    private AbstractReadFileHandle currWalSegment;

    /** Logger */
    @NotNull protected final IgniteLogger log;

    /**
     * Shared context for creating serializer of required version and grid name access. Also cacheObjects processor from
     * this context may be used to covert Data entry key and value from its binary representation into objects.
     */
    @NotNull protected final GridCacheSharedContext sharedCtx;

    /** Serializer factory. */
    @NotNull private final RecordSerializerFactory serializerFactory;

    /** Factory to provide I/O interfaces for read/write operations with files */
    @NotNull protected final FileIOFactory ioFactory;

    /** Utility buffer for reading records */
    private final ByteBufferExpander buf;

    /**
     * @param log Logger.
     * @param sharedCtx Shared context.
     * @param serializerFactory Serializer of current version to read headers.
     * @param ioFactory ioFactory for file IO access.
     * @param initialReadBufferSize buffer for reading records size.
     */
    protected AbstractWalRecordsIterator(
        @NotNull final IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx,
        @NotNull final RecordSerializerFactory serializerFactory,
        @NotNull final FileIOFactory ioFactory,
        final int initialReadBufferSize
    ) {
        this.log = log;
        this.sharedCtx = sharedCtx;
        this.serializerFactory = serializerFactory;
        this.ioFactory = ioFactory;

        buf = new ByteBufferExpander(initialReadBufferSize, ByteOrder.nativeOrder());
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

                if (curRec != null) {
                    if (curRec.get2().type() == null)
                        continue; // Record was skipped by filter of current serializer, should read next record.

                    return;
                }
                else {
                    currWalSegment = advanceSegment(currWalSegment);

                    if (currWalSegment == null)
                        return;
                }
            }
            catch (WalSegmentTailReachedException e) {
                AbstractReadFileHandle currWalSegment = this.currWalSegment;

                IgniteCheckedException e0 = validateTailReachedException(e, currWalSegment);

                if (e0 != null)
                    throw e0;

                log.warning(e.getMessage());

                curRec = null;

                return;
            }
        }
    }

    /**
     *
     * @param tailReachedException Tail reached exception.
     * @param currWalSegment Current WAL segment read handler.
     * @return If need to throw exception after validation.
     */
    protected IgniteCheckedException validateTailReachedException(
        WalSegmentTailReachedException tailReachedException,
        AbstractReadFileHandle currWalSegment
    ) {
        return !currWalSegment.workDir() ? new IgniteCheckedException(
            "WAL tail reached in archive directory, " +
                "WAL segment file is corrupted.", tailReachedException) : null;
    }

    /**
     * Closes and returns WAL segment (if any)
     *
     * @return closed handle
     * @throws IgniteCheckedException if IO failed
     */
    @Nullable protected AbstractReadFileHandle closeCurrentWalSegment() throws IgniteCheckedException {
        final AbstractReadFileHandle walSegmentClosed = currWalSegment;

        if (walSegmentClosed != null) {
            walSegmentClosed.close();
            currWalSegment = null;
        }
        return walSegmentClosed;
    }

    /**
     * Switches records iterator to the next WAL segment
     * as result of this method, new reference to segment should be returned.
     * Null for current handle means stop of iteration.
     *
     * @param curWalSegment current open WAL segment or null if there is no open segment yet
     * @return new WAL segment to read or null for stop iteration
     * @throws IgniteCheckedException if reading failed
     */
    protected abstract AbstractReadFileHandle advanceSegment(
        @Nullable final AbstractReadFileHandle curWalSegment
    ) throws IgniteCheckedException;

    /**
     * Switches to new record.
     *
     * @param hnd currently opened read handle.
     * @return next advanced record.
     */
    private IgniteBiTuple<WALPointer, WALRecord> advanceRecord(
        @Nullable final AbstractReadFileHandle hnd
    ) throws IgniteCheckedException {
        if (hnd == null)
            return null;

        FileWALPointer actualFilePtr = new FileWALPointer(hnd.idx(), (int)hnd.in().position(), 0);

        try {
            WALRecord rec = hnd.ser().readRecord(hnd.in(), actualFilePtr);

            actualFilePtr.length(rec.size());

            // cast using diamond operator here can break compile for 7
            return new IgniteBiTuple<>((WALPointer)actualFilePtr, postProcessRecord(rec));
        }
        catch (IOException | IgniteCheckedException e) {
            if (e instanceof WalSegmentTailReachedException) {
                throw new WalSegmentTailReachedException(
                    "WAL segment tail reached. [idx=" + hnd.idx() +
                        ", isWorkDir=" + hnd.workDir() + ", serVer=" + hnd.ser() + "]", e);
            }

            if (!(e instanceof SegmentEofException) && !(e instanceof EOFException)) {
                IgniteCheckedException e0 = handleRecordException(e, actualFilePtr);

                if (e0 != null)
                    throw e0;
            }

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
     * Handler for record deserialization exception.
     *
     * @param e problem from records reading
     * @param ptr file pointer was accessed
     *
     * @return {@code null} if the error was handled and we can go ahead,
     *  {@code IgniteCheckedException} if the error was not handled, and we should stop the iteration.
     */
    protected IgniteCheckedException handleRecordException(@NotNull final Exception e, @Nullable final FileWALPointer ptr) {
        if (log.isInfoEnabled())
            log.info("Stopping WAL iteration due to an exception: " + e.getMessage() + ", ptr=" + ptr);

        return new IgniteCheckedException(e);
    }

    /**
     * @param desc File descriptor.
     * @param start Optional start pointer. Null means read from the beginning
     * @return Initialized file handle.
     * @throws FileNotFoundException If segment file is missing.
     * @throws IgniteCheckedException If initialized failed due to another unexpected error.
     */
    protected AbstractReadFileHandle initReadHandle(
        @NotNull final AbstractFileDescriptor desc,
        @Nullable final FileWALPointer start
    ) throws IgniteCheckedException, FileNotFoundException {
        try {
            FileIO fileIO = desc.isCompressed() ? new UnzipFileIO(desc.file()) : ioFactory.create(desc.file());

            try {
                SegmentHeader segmentHeader = readSegmentHeader(fileIO, curWalSegmIdx);

                boolean isCompacted = segmentHeader.isCompacted();

                if (isCompacted)
                    serializerFactory.skipPositionCheck(true);

                FileInput in = new FileInput(fileIO, buf);

                if (start != null && desc.idx() == start.index()) {
                    if (isCompacted) {
                        if (start.fileOffset() != 0)
                            serializerFactory.recordDeserializeFilter(new StartSeekingFilter(start));
                    }
                    else {
                        // Make sure we skip header with serializer version.
                        long startOff = Math.max(start.fileOffset(), fileIO.position());

                        in.seek(startOff);
                    }
                }

                int serVer = segmentHeader.getSerializerVersion();

                return createReadFileHandle(fileIO, desc.idx(), serializerFactory.createSerializer(serVer), in);
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
                "Failed to initialize WAL segment: " + desc.file().getAbsolutePath(), e);
        }
    }

    /** */
    protected abstract AbstractReadFileHandle createReadFileHandle(
        FileIO fileIO,
        long idx,
        RecordSerializer ser,
        FileInput in
    );

    /**
     * Filter that drops all records until given start pointer is reached.
     */
    private static class StartSeekingFilter implements P2<WALRecord.RecordType, WALPointer> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Start pointer. */
        private final FileWALPointer start;

        /** Start reached flag. */
        private boolean startReached;

        /**
         * @param start Start.
         */
        StartSeekingFilter(FileWALPointer start) {
            this.start = start;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(WALRecord.RecordType type, WALPointer pointer) {
            if (start.fileOffset() == ((FileWALPointer)pointer).fileOffset())
                startReached = true;

            return startReached;
        }
    }

    /** */
    protected interface AbstractReadFileHandle {
        /** */
        void close() throws IgniteCheckedException;

        /** */
        long idx();

        /** */
        FileInput in();

        /** */
        RecordSerializer ser();

        /** */
        boolean workDir();
    }

    /** */
    protected interface AbstractFileDescriptor {
        /** */
        boolean isCompressed();

        /** */
        File file();

        /** */
        long idx();
    }
}
