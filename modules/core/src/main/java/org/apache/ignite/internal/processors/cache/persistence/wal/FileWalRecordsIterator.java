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
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.SegmentHeader;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readSegmentHeader;

/**
 * Iterator over WAL segments. This abstract class provides most functionality for reading records in log. Subclasses
 * are to override segment switching functionality
 */
public abstract class FileWalRecordsIterator extends AbstractWalRecordsIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Current WAL segment absolute index. <br> Determined as lowest number of file at start, is changed during advance
     * segment
     */
    protected long curWalSegmIdx = -1;

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

    /** Factory to provide I/O interfaces for read primitives with files. */
    private final SegmentFileInputFactory segmentFileInputFactory;

    /**
     * @param log Logger.
     * @param sharedCtx Shared context.
     * @param serializerFactory Serializer of current version to read headers.
     * @param ioFactory ioFactory for file IO access.
     * @param initialReadBufferSize buffer for reading records size.
     * @param segmentFileInputFactory Factory to provide I/O interfaces for read primitives with files.
     */
    protected FileWalRecordsIterator(
        @NotNull final IgniteLogger log,
        @NotNull final GridCacheSharedContext sharedCtx,
        @NotNull final RecordSerializerFactory serializerFactory,
        @NotNull final FileIOFactory ioFactory,
        final int initialReadBufferSize,
        SegmentFileInputFactory segmentFileInputFactory) {
        super(log);
        this.sharedCtx = sharedCtx;
        this.serializerFactory = serializerFactory;
        this.ioFactory = ioFactory;
        this.segmentFileInputFactory = segmentFileInputFactory;

        buf = new ByteBufferExpander(initialReadBufferSize, ByteOrder.nativeOrder());
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

    /** {@inheritDoc} */
    @Override protected IgniteCheckedException validateTailReachedException(
        WalSegmentTailReachedException tailReachedException,
        AbstractWalSegmentHandle currWalSegment
    ) {
        if (currWalSegment instanceof AbstractReadFileHandle && !((AbstractReadFileHandle)currWalSegment).workDir())
            return new IgniteCheckedException(
                "WAL tail reached in archive directory, " +
                    "WAL segment file is corrupted.",
                tailReachedException);
        return null;

    }

    /**
     * Closes and returns WAL segment (if any)
     *
     * @return closed handle
     * @throws IgniteCheckedException if IO failed
     */
    @Nullable protected AbstractWalRecordsIterator.AbstractWalSegmentHandle closeCurrentWalSegment() throws IgniteCheckedException {
        final AbstractWalSegmentHandle walSegmentClosed = currWalSegment;

        if (walSegmentClosed != null) {
            walSegmentClosed.close();
            currWalSegment = null;
        }
        return walSegmentClosed;
    }

    /** {@inheritDoc} */
    @Override protected abstract AbstractWalSegmentHandle advanceSegment(
        @Nullable final AbstractWalRecordsIterator.AbstractWalSegmentHandle curWalSegment
    ) throws IgniteCheckedException;

    /**
     * Assumes fileIO will be closed in this method in case of error occurred.
     *
     * @param desc File descriptor.
     * @param start Optional start pointer. Null means read from the beginning.
     * @param fileIO fileIO associated with file descriptor
     * @param segmentHeader read segment header from fileIO
     * @return Initialized file read header.
     * @throws IgniteCheckedException If initialized failed due to another unexpected error.
     */
    protected AbstractWalSegmentHandle initReadHandle(
        @NotNull final AbstractFileDescriptor desc,
        @Nullable final WALPointer start,
        @NotNull final SegmentIO fileIO,
        @NotNull final SegmentHeader segmentHeader
    ) throws IgniteCheckedException {
        try {
            boolean isCompacted = segmentHeader.isCompacted();

            if (isCompacted)
                serializerFactory.skipPositionCheck(true);

            FileInput in = segmentFileInputFactory.createFileInput(fileIO, buf);

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

            return createReadFileHandle(fileIO, serializerFactory.createSerializer(serVer), in);
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
        catch (IgniteCheckedException e) {
            U.closeWithSuppressingException(fileIO, e);

            throw e;
        }
        catch (IOException e) {
            U.closeWithSuppressingException(fileIO, e);

            throw new IgniteCheckedException(
                "Failed to initialize WAL segment after reading segment header: " + desc.file().getAbsolutePath(), e);
        }
    }

    /**
     * Assumes file descriptor will be opened in this method. The caller of this method must be responsible for closing
     * opened file descriptor File descriptor will be closed ONLY in case of error occurred.
     *
     * @param desc File descriptor.
     * @param start Optional start pointer. Null means read from the beginning
     * @return Initialized file read header.
     * @throws FileNotFoundException If segment file is missing.
     * @throws IgniteCheckedException If initialized failed due to another unexpected error.
     */
    protected AbstractWalSegmentHandle initReadHandle(
        @NotNull final AbstractFileDescriptor desc,
        @Nullable final WALPointer start
    ) throws IgniteCheckedException, FileNotFoundException {
        SegmentIO fileIO = null;

        try {
            fileIO = desc.toReadOnlyIO(ioFactory);

            SegmentHeader segmentHeader;

            try {
                segmentHeader = readSegmentHeader(fileIO, segmentFileInputFactory);
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
                U.closeWithSuppressingException(fileIO, e);

                throw e;
            }

            return initReadHandle(desc, start, fileIO, segmentHeader);
        }
        catch (FileNotFoundException e) {
            U.closeQuiet(fileIO);

            throw e;
        }
        catch (IOException e) {
            U.closeQuiet(fileIO);

            throw new IgniteCheckedException(
                "Failed to initialize WAL segment: " + desc.file().getAbsolutePath(), e);
        }
    }

    /** */
    protected abstract AbstractWalSegmentHandle createReadFileHandle(
        SegmentIO fileIO,
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
        private final WALPointer start;

        /** Start reached flag. */
        private boolean startReached;

        /**
         * @param start Start.
         */
        StartSeekingFilter(WALPointer start) {
            this.start = start;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(WALRecord.RecordType type, WALPointer pointer) {
            if (start.fileOffset() == pointer.fileOffset())
                startReached = true;

            return startReached;
        }
    }

    /** */
    protected interface AbstractReadFileHandle extends AbstractWalSegmentHandle {
        /** */
        @Override FileInput in();

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

        /**
         * Make fileIo by this description.
         *
         * @param fileIOFactory Factory for fileIo creation.
         * @return One of implementation of {@link FileIO}.
         * @throws IOException if creation of fileIo was not success.
         */
        SegmentIO toReadOnlyIO(FileIOFactory fileIOFactory) throws IOException;
    }
}
