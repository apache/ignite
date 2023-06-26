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
import java.io.IOException;
import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterator over WAL segments. This abstract class provides most functionality for reading records.
 */
public abstract class AbstractWalRecordsIterator
    extends GridCloseableIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>> implements WALIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Current record preloaded, to be returned on next()<br> Normally this should be not null because advance() method
     * should already prepare some value<br>
     */
    protected IgniteBiTuple<WALPointer, WALRecord> curRec;

    /**
     * The exception which can be thrown during reading next record. It holds until the next calling of next record.
     */
    private IgniteCheckedException curException;

    /**
     * Current WAL segment read file handle. To be filled by subclass advanceSegment
     */
    private transient AbstractWalSegmentHandle currWalSegment;

    /** Logger */
    @NotNull protected final IgniteLogger log;

    /** Position of last read valid record. */
    protected WALPointer lastRead;

    /**
     * @param log Logger.
     */
    protected AbstractWalRecordsIterator(@NotNull final IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<WALPointer, WALRecord> onNext() throws IgniteCheckedException {
        if (curException != null)
            throw curException;

        IgniteBiTuple<WALPointer, WALRecord> ret = curRec;

        try {
            advance();
        }
        catch (IgniteCheckedException e) {
            curException = e;
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        if (curException != null)
            throw curException;

        return curRec != null;
    }

    /**
     * Switches records iterator to the next record. <ul> <li>{@link #curRec} will be updated.</li> <li> If end of
     * segment reached, switch to new segment is called. {@link #currWalSegment} will be updated.</li> </ul>
     *
     * {@code advance()} runs a step ahead {@link #next()}
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void advance() throws IgniteCheckedException {
        if (curRec != null)
            lastRead = curRec.get1();

        while (true) {
            try {
                curRec = advanceRecord(currWalSegment);

                if (curRec != null) {
                    if (curRec.get2().type() == null) {
                        lastRead = curRec.get1();

                        continue; // Record was skipped by filter of current serializer, should read next record.
                    }

                    return;
                }
                else {
                    currWalSegment = advanceSegment(currWalSegment);

                    if (currWalSegment == null)
                        return;
                }
            }
            catch (WalSegmentTailReachedException e) {

                IgniteCheckedException e0 = validateTailReachedException(e, currWalSegment);

                if (e0 != null)
                    throw e0;

                log.warning(e.getMessage());

                curRec = null;

                return;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Optional<WALPointer> lastRead() {
        return Optional.ofNullable(lastRead);
    }

    /**
     * @param tailReachedException Tail reached exception.
     * @param currWalSegment Current WAL segment read handler.
     * @return If need to throw exception after validation.
     */
    protected abstract IgniteCheckedException validateTailReachedException(
        WalSegmentTailReachedException tailReachedException,
        AbstractWalSegmentHandle currWalSegment);

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

    /**
     * Switches records iterator to the next WAL segment as result of this method, new reference to segment should be
     * returned. Null for current handle means stop of iteration.
     *
     * @param segment current open WAL segment or null if there is no open segment yet
     * @return new WAL segment to read or null for stop iteration
     * @throws IgniteCheckedException if reading failed
     */
    protected abstract AbstractWalSegmentHandle advanceSegment(
        @Nullable final AbstractWalSegmentHandle segment
    ) throws IgniteCheckedException;

    /**
     * Switches to new record.
     *
     * @param hnd currently opened read handle.
     * @return next advanced record.
     */
    protected IgniteBiTuple<WALPointer, WALRecord> advanceRecord(
        @Nullable final AbstractWalSegmentHandle hnd
    ) throws IgniteCheckedException {
        if (hnd == null)
            return null;

        WALPointer actualFilePtr = new WALPointer(hnd.idx(), (int)hnd.in().position(), 0);

        try {
            WALRecord rec = hnd.ser().readRecord(hnd.in(), actualFilePtr);

            actualFilePtr.length(rec.size());

            // cast using diamond operator here can break compile for 7
            return new IgniteBiTuple<>(actualFilePtr, postProcessRecord(rec));
        }
        catch (IOException | IgniteCheckedException e) {
            if (e instanceof WalSegmentTailReachedException) {
                boolean workDir = hnd instanceof FileWalRecordsIterator.AbstractReadFileHandle
                    && ((FileWalRecordsIterator.AbstractReadFileHandle)hnd).workDir();
                throw new WalSegmentTailReachedException(
                    "WAL segment tail reached. [idx=" + hnd.idx() +
                        ", isWorkDir=" + workDir + ", serVer=" + hnd.ser() +
                        ", actualFilePtr=" + actualFilePtr + ']',
                    e
                );
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
     * Performs final conversions with record loaded from WAL. To be overridden by subclasses if any processing
     * required.
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
     * @return {@code null} if the error was handled and we can go ahead, {@code IgniteCheckedException} if the error
     * was not handled, and we should stop the iteration.
     */
    protected IgniteCheckedException handleRecordException(
        @NotNull final Exception e,
        @Nullable final WALPointer ptr
    ) {
        if (log.isInfoEnabled())
            log.info("Stopping WAL iteration due to an exception: " + e.getMessage() + ", ptr=" + ptr);

        return new IgniteCheckedException(e);
    }

    /** */
    protected interface AbstractWalSegmentHandle {
        /** */
        void close() throws IgniteCheckedException;

        /** */
        long idx();

        /** */
        ByteBufferBackedDataInput in();

        /** */
        RecordSerializer ser();
    }
}
