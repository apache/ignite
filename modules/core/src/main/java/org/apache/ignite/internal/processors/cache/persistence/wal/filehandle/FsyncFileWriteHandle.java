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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.SwitchSegmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.prepareSerializerVersionBuffer;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory.LATEST_SERIALIZER_VERSION;

/**
 * File handle for one log segment.
 */
@SuppressWarnings("SignalWithoutCorrespondingAwait")
class FsyncFileWriteHandle extends AbstractFileHandle implements FileWriteHandle {
    /** */
    private final RecordSerializer serializer;

    /** Max segment size. */
    private final long maxSegmentSize;

    /** Serializer latest version to use. */
    private final int serializerVersion =
        IgniteSystemProperties.getInteger(IGNITE_WAL_SERIALIZER_VERSION, LATEST_SERIALIZER_VERSION);

    /**
     * Accumulated WAL records chain. This reference points to latest WAL record. When writing records chain is iterated
     * from latest to oldest (see {@link WALRecord#previous()}) Records from chain are saved into buffer in reverse
     * order
     */
    final AtomicReference<WALRecord> head = new AtomicReference<>();

    /**
     * Position in current file after the end of last written record (incremented after file channel write operation)
     */
    private volatile long written;

    /** */
    private volatile long lastFsyncPos;

    /** Stop guard to provide warranty that only one thread will be successful in calling {@link #close(boolean)} */
    private final AtomicBoolean stop = new AtomicBoolean(false);

    /** */
    private final Lock lock = new ReentrantLock();

    /** Condition activated each time writeBuffer() completes. Used to wait previously flushed write to complete */
    private final Condition writeComplete = lock.newCondition();

    /** Condition for timed wait of several threads, see {@link DataStorageConfiguration#getWalFsyncDelayNanos()} */
    private final Condition fsync = lock.newCondition();

    /**
     * Next segment available condition. Protection from "spurious wakeup" is provided by predicate {@link
     * #fileIO}=<code>null</code>
     */
    private final Condition nextSegment = lock.newCondition();

    /** */
    private final WALMode mode;

    /** Thread local byte buffer size, see {@link #tlb} */
    private final int tlbSize;

    /** Context. */
    protected final GridCacheSharedContext cctx;

    /** Persistence metrics tracker. */
    private final DataStorageMetricsImpl metrics;

    /** Logger. */
    protected final IgniteLogger log;

    /** Fsync delay. */
    private final long fsyncDelay;

    /** Switch segment record offset. */
    private int switchSegmentRecordOffset;

    /**
     * Thread local byte buffer for saving serialized WAL records chain, see {@link FsyncFileWriteHandle#head}.
     * Introduced to decrease number of buffers allocation. Used only for record itself is shorter than {@link
     * #tlbSize}.
     */
    private final ThreadLocal<ByteBuffer> tlb = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocateDirect(tlbSize);

            buf.order(GridUnsafe.NATIVE_BYTE_ORDER);

            return buf;
        }
    };

    /**
     * @param cctx Context.
     * @param fileIO I/O file interface to use.
     * @param metrics Data storage metrics.
     * @param serializer Serializer.
     * @param pos Position.
     * @param mode WAL mode.
     * @param maxSegmentSize Max segment size.
     * @param size Thread local byte buffer size.
     * @param fsyncDelay Fsync delay.
     * @throws IOException If failed.
     */
    FsyncFileWriteHandle(
        GridCacheSharedContext cctx, SegmentIO fileIO,
        DataStorageMetricsImpl metrics, RecordSerializer serializer, long pos,
        WALMode mode, long maxSegmentSize, int size, long fsyncDelay) throws IOException {
        super(fileIO);
        assert serializer != null;

        this.mode = mode;
        tlbSize = size;
        this.cctx = cctx;
        this.metrics = metrics;
        this.log = cctx.logger(FsyncFileWriteHandle.class);
        this.fsyncDelay = fsyncDelay;
        this.maxSegmentSize = maxSegmentSize;
        this.serializer = serializer;
        this.written = pos;
        this.lastFsyncPos = pos;

        head.set(new FakeRecord(new FileWALPointer(fileIO.getSegmentId(), (int)pos, 0), false));

        fileIO.position(pos);
    }

    /** {@inheritDoc} */
    @Override public int serializerVersion() {
        return serializer.version();
    }

    /** {@inheritDoc} */
    @Override public void finishResumeLogging() {
        // NOOP.
    }

    /**
     * Write serializer version to current handle. NOTE: Method mutates {@code fileIO} position, written and
     * lastFsyncPos fields.
     *
     * @throws StorageException If fail to write serializer version.
     */
    @Override public void writeHeader() throws StorageException {
        try {
            assert fileIO.position() == 0 : "Serializer version can be written only at the begin of file " +
                fileIO.position();

            long updatedPosition = writeSerializerVersion(fileIO, getSegmentId(),
                serializer.version(), mode);

            written = updatedPosition;
            lastFsyncPos = updatedPosition;
            head.set(new FakeRecord(new FileWALPointer(getSegmentId(), (int)updatedPosition, 0), false));
        }
        catch (IOException e) {
            throw new StorageException("Unable to write serializer version for segment " + getSegmentId(), e);
        }
    }

    /**
     * Writes record serializer version to provided {@code io}. NOTE: Method mutates position of {@code io}.
     *
     * @param io I/O interface for file.
     * @param idx Segment index.
     * @param version Serializer version.
     * @return I/O position after write version.
     * @throws IOException If failed to write serializer version.
     */
    private static long writeSerializerVersion(FileIO io, long idx, int version, WALMode mode) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(RecordV1Serializer.HEADER_RECORD_SIZE);
        buf.order(ByteOrder.nativeOrder());

        io.writeFully(prepareSerializerVersionBuffer(idx, version, false, buf));

        // Flush
        if (mode == WALMode.FSYNC)
            io.force();

        return io.position();
    }

    /**
     * Checks if current head is a close fake record and returns {@code true} if so.
     *
     * @return {@code true} if current head is close record.
     */
    private boolean stopped() {
        return stopped(head.get());
    }

    /**
     * @param record Record to check.
     * @return {@code true} if the record is fake close record.
     */
    private boolean stopped(WALRecord record) {
        return record instanceof FakeRecord && ((FakeRecord)record).stop;
    }

    /** {@inheritDoc} */
    @Nullable @Override public WALPointer addRecord(WALRecord rec) throws StorageException {
        assert rec.size() > 0 || rec.getClass() == FakeRecord.class;

        boolean flushed = false;

        for (; ; ) {
            WALRecord h = head.get();

            long nextPos = nextPosition(h);

            if (nextPos + rec.size() >= maxSegmentSize || stopped(h)) {
                // Can not write to this segment, need to switch to the next one.
                return null;
            }

            int newChainSize = h.chainSize() + rec.size();

            if (newChainSize > tlbSize && !flushed) {
                boolean res = h.previous() == null || flush(h, false);

                if (rec.size() > tlbSize)
                    flushed = res;

                continue;
            }

            rec.chainSize(newChainSize);
            rec.previous(h);

            FileWALPointer ptr = new FileWALPointer(
                getSegmentId(),
                (int)nextPos,
                rec.size());

            rec.position(ptr);

            if (head.compareAndSet(h, rec))
                return ptr;
        }
    }

    /** {@inheritDoc} */
    @Override public void flushAll() throws IgniteCheckedException {
        flush(head.get(), false);
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void flushAllOnStop() throws IgniteCheckedException {
        flush(head.get(), true);
    }

    /**
     * @param rec Record.
     * @return Position for the next record.
     */
    private long nextPosition(WALRecord rec) {
        return recordOffset(rec) + rec.size();
    }

    /**
     * Gets WAL record offset relative to the WAL segment file beginning.
     *
     * @param rec WAL record.
     * @return File offset.
     */
    private static int recordOffset(WALRecord rec) {
        FileWALPointer ptr = (FileWALPointer)rec.position();

        assert ptr != null;

        return ptr.fileOffset();
    }

    /**
     * Flush or wait for concurrent flush completion.
     *
     * @param ptr Pointer.
     * @throws StorageException If failed.
     */
    private void flushOrWait(FileWALPointer ptr, boolean stop) throws StorageException {
        long expWritten;

        if (ptr != null) {
            // If requested obsolete file index, it must be already flushed by close.
            if (ptr.index() != getSegmentId())
                return;

            expWritten = ptr.fileOffset();
        }
        else // We read head position before the flush because otherwise we can get wrong position.
            expWritten = recordOffset(head.get());

        if (flush(ptr, stop))
            return;
        else if (stop) {
            FakeRecord fr = (FakeRecord)head.get();

            assert fr.stop : "Invalid fake record on top of the queue: " + fr;

            expWritten = recordOffset(fr);
        }

        // Spin-wait for a while before acquiring the lock.
        for (int i = 0; i < 64; i++) {
            if (written >= expWritten)
                return;
        }

        // If we did not flush ourselves then await for concurrent flush to complete.
        lock.lock();

        try {
            while (written < expWritten && !cctx.kernalContext().invalid())
                U.awaitQuiet(writeComplete);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param ptr Pointer.
     * @return {@code true} If the flush really happened.
     * @throws StorageException If failed.
     */
    private boolean flush(FileWALPointer ptr, boolean stop) throws StorageException {
        if (ptr == null) { // Unconditional flush.
            for (; ; ) {
                WALRecord expHead = head.get();

                if (expHead.previous() == null) {
                    FakeRecord frHead = (FakeRecord)expHead;

                    if (frHead.stop == stop || frHead.stop ||
                        head.compareAndSet(expHead, new FakeRecord(frHead.position(), stop)))
                        return false;
                }

                if (flush(expHead, stop))
                    return true;
            }
        }

        assert ptr.index() == getSegmentId();

        for (; ; ) {
            WALRecord h = head.get();

            // If current chain begin position is greater than requested, then someone else flushed our changes.
            if (chainBeginPosition(h) > ptr.fileOffset())
                return false;

            if (flush(h, stop))
                return true; // We are lucky.
        }
    }

    /**
     * @param h Head of the chain.
     * @return Chain begin position.
     */
    private long chainBeginPosition(WALRecord h) {
        return recordOffset(h) + h.size() - h.chainSize();
    }

    /**
     * @throws StorageException If node is no longer valid and we missed a WAL operation.
     */
    private void checkNode() throws StorageException {
        if (cctx.kernalContext().invalid())
            throw new StorageException("Failed to perform WAL operation (environment was invalidated by a " +
                "previous error)");
    }

    /**
     * @param expHead Expected head of chain. If head was changed, flush is not performed in this thread
     * @throws StorageException If failed.
     */
    private boolean flush(WALRecord expHead, boolean stop) throws StorageException {
        if (expHead.previous() == null) {
            FakeRecord frHead = (FakeRecord)expHead;

            if (!stop || frHead.stop) // Protects from CASing terminal FakeRecord(true) to FakeRecord(false)
                return false;
        }

        // Fail-fast before CAS.
        checkNode();

        if (!head.compareAndSet(expHead, new FakeRecord(new FileWALPointer(getSegmentId(), (int)nextPosition(expHead), 0), stop)))
            return false;

        if (expHead.chainSize() == 0)
            return false;

        // At this point we grabbed the piece of WAL chain.
        // Any failure in this code must invalidate the environment.
        try {
            // We can safely allow other threads to start building next chains while we are doing flush here.
            ByteBuffer buf;

            boolean tmpBuf = false;

            if (expHead.chainSize() > tlbSize) {
                buf = GridUnsafe.allocateBuffer(expHead.chainSize());

                tmpBuf = true; // We need to manually release this temporary direct buffer.
            }
            else
                buf = tlb.get();

            try {
                long pos = fillBuffer(buf, expHead);

                writeBuffer(pos, buf);
            }
            finally {
                if (tmpBuf)
                    GridUnsafe.freeBuffer(buf);
            }

            return true;
        }
        catch (Throwable e) {
            StorageException se = e instanceof StorageException ? (StorageException)e :
                new StorageException("Unable to write", new IOException(e));

            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, se));

            // All workers waiting for a next segment must be woken up and stopped
            signalNextAvailable();

            throw se;
        }
    }

    /**
     * Serializes WAL records chain to provided byte buffer.
     *
     * @param buf Buffer, will be filled with records chain from end to beginning.
     * @param head Head of the chain to write to the buffer.
     * @return Position in file for this buffer.
     * @throws IgniteCheckedException If failed.
     */
    private long fillBuffer(ByteBuffer buf, WALRecord head) throws IgniteCheckedException {
        final int limit = head.chainSize();

        assert limit <= buf.capacity();

        buf.rewind();
        buf.limit(limit);

        do {
            buf.position(head.chainSize() - head.size());
            buf.limit(head.chainSize()); // Just to make sure that serializer works in bounds.

            try {
                serializer.writeRecord(head, buf);
            }
            catch (RuntimeException e) {
                throw new IllegalStateException("Failed to write record: " + head, e);
            }

            assert !buf.hasRemaining() : "Reported record size is greater than actual: " + head;

            head = head.previous();
        }
        while (head.previous() != null);

        assert head instanceof FakeRecord : head.getClass();

        buf.rewind();
        buf.limit(limit);

        return recordOffset(head);
    }

    /**
     * Non-blocking check if this pointer needs to be sync'ed.
     *
     * @param ptr WAL pointer to check.
     * @return {@code False} if this pointer has been already sync'ed.
     */
    @Override public boolean needFsync(FileWALPointer ptr) {
        // If index has changed, it means that the log was rolled over and already sync'ed.
        // If requested position is smaller than last sync'ed, it also means all is good.
        // If position is equal, then our record is the last not synced.
        return getSegmentId() == ptr.index() && lastFsyncPos <= ptr.fileOffset();
    }

    /** {@inheritDoc} */
    @Override public FileWALPointer position() {
        lock.lock();

        try {
            return new FileWALPointer(getSegmentId(), (int)written, 0);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void fsync(FileWALPointer ptr) throws StorageException, IgniteCheckedException {
        fsync(ptr, false);
    }

    /** {@inheritDoc} */
    @Override public void closeBuffer() {
        //NOOP.
    }

    /**
     * @param ptr Pointer to sync.
     * @throws StorageException If failed.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    protected void fsync(FileWALPointer ptr, boolean stop) throws StorageException, IgniteInterruptedCheckedException {
        lock.lock();

        try {
            if (ptr != null) {
                if (!needFsync(ptr))
                    return;

                if (fsyncDelay > 0 && !stopped()) {
                    // Delay fsync to collect as many updates as possible: trade latency for throughput.
                    U.await(fsync, fsyncDelay, TimeUnit.NANOSECONDS);

                    if (!needFsync(ptr))
                        return;
                }
            }

            flushOrWait(ptr, stop);

            if (stopped())
                return;

            if (lastFsyncPos != written) {
                assert lastFsyncPos < written; // Fsync position must be behind.

                boolean metricsEnabled = metrics.metricsEnabled();

                long start = metricsEnabled ? System.nanoTime() : 0;

                try {
                    fileIO.force();
                }
                catch (IOException e) {
                    throw new StorageException(e);
                }

                lastFsyncPos = written;

                if (fsyncDelay > 0)
                    fsync.signalAll();

                long end = metricsEnabled ? System.nanoTime() : 0;

                if (metricsEnabled)
                    metrics.onFsync(end - start);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return {@code true} If this thread actually closed the segment.
     * @throws StorageException If failed.
     */
    @Override public boolean close(boolean rollOver) throws StorageException {
        if (stop.compareAndSet(false, true)) {
            lock.lock();

            try {
                flushOrWait(null, true);

                assert stopped() : "Segment is not closed after close flush: " + head.get();

                try {
                    try {
                        RecordSerializer backwardSerializer = new RecordSerializerFactoryImpl(cctx)
                            .createSerializer(serializerVersion);

                        SwitchSegmentRecord segmentRecord = new SwitchSegmentRecord();

                        int switchSegmentRecSize = backwardSerializer.size(segmentRecord);

                        if (rollOver && written + switchSegmentRecSize < maxSegmentSize) {
                            final ByteBuffer buf = ByteBuffer.allocate(switchSegmentRecSize);

                            segmentRecord.position(new FileWALPointer(getSegmentId(), (int)written, switchSegmentRecSize));
                            backwardSerializer.writeRecord(segmentRecord, buf);

                            buf.rewind();

                            written += fileIO.writeFully(buf, written);

                            switchSegmentRecordOffset = (int)written;
                        }
                    }
                    catch (IgniteCheckedException e) {
                        throw new IOException(e);
                    }
                    finally {
                        assert mode == WALMode.FSYNC;

                        // Do the final fsync.
                        fileIO.force();

                        lastFsyncPos = written;

                        fileIO.close();
                    }
                }
                catch (IOException e) {
                    throw new StorageException("Failed to close WAL write handle [idx=" + getSegmentId() + "]", e);
                }

                if (log.isDebugEnabled())
                    log.debug("Closed WAL write handle [idx=" + getSegmentId() + "]");

                return true;
            }
            finally {
                lock.unlock();
            }
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override public void signalNextAvailable() {
        lock.lock();

        try {
            WALRecord rec = head.get();

            if (!cctx.kernalContext().invalid()) {
                assert rec instanceof FakeRecord : "Expected head FakeRecord, actual head "
                    + (rec != null ? rec.getClass().getSimpleName() : "null");

                assert written == lastFsyncPos || mode != WALMode.FSYNC :
                    "fsync [written=" + written + ", lastFsync=" + lastFsyncPos + ']';

                fileIO = null;
            }
            else {
                try {
                    fileIO.close();
                }
                catch (IOException e) {
                    U.error(log, "Failed to close WAL file [idx=" + getSegmentId() + ", fileIO=" + fileIO + "]", e);
                }
            }

            nextSegment.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void awaitNext() {
        lock.lock();

        try {
            while (fileIO != null && !cctx.kernalContext().invalid())
                U.awaitQuiet(nextSegment);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param pos Position in file to start write from. May be checked against actual position to wait previous writes
     * to complete.
     * @param buf Buffer to write to file.
     * @throws StorageException If failed.
     */
    @SuppressWarnings("TooBroadScope")
    private void writeBuffer(long pos, ByteBuffer buf) throws StorageException {
        boolean interrupted = false;

        lock.lock();

        try {
            assert fileIO != null : "Writing to a closed segment.";

            checkNode();

            long lastLogged = U.currentTimeMillis();

            long logBackoff = 2_000;

            // If we were too fast, need to wait previous writes to complete.
            while (written != pos) {
                assert written < pos : "written = " + written + ", pos = " + pos; // No one can write further than we are now.

                // Permutation occurred between blocks write operations.
                // Order of acquiring lock is not the same as order of write.
                long now = U.currentTimeMillis();

                if (now - lastLogged >= logBackoff) {
                    if (logBackoff < 60 * 60_000)
                        logBackoff *= 2;

                    U.warn(log, "Still waiting for a concurrent write to complete [written=" + written +
                        ", pos=" + pos + ", lastFsyncPos=" + lastFsyncPos + ", stop=" + stop.get() +
                        ", actualPos=" + safePosition() + ']');

                    lastLogged = now;
                }

                try {
                    writeComplete.await(2, TimeUnit.SECONDS);
                }
                catch (InterruptedException ignore) {
                    interrupted = true;
                }

                checkNode();
            }

            // Do the write.
            int size = buf.remaining();

            assert size > 0 : size;

            try {
                assert written == fileIO.position();

                fileIO.writeFully(buf);

                written += size;

                metrics.onWalBytesWritten(size);

                assert written == fileIO.position();
            }
            catch (IOException e) {
                StorageException se = new StorageException("Unable to write", e);

                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, se));

                throw se;
            }
        }
        finally {
            writeComplete.signalAll();

            lock.unlock();

            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * @return Safely reads current position of the file channel as String. Will return "null" if channel is null.
     */
    public String safePosition() {
        FileIO io = this.fileIO;

        if (io == null)
            return "null";

        try {
            return String.valueOf(io.position());
        }
        catch (IOException e) {
            return "{Failed to read channel position: " + e.getMessage() + "}";
        }
    }

    /** {@inheritDoc} */
    @Override public int getSwitchSegmentRecordOffset() {
        return switchSegmentRecordOffset;
    }

    /**
     * Fake record is zero-sized record, which is not stored into file. Fake record is used for storing position in file
     * {@link WALRecord#position()}. Fake record is allowed to have no previous record.
     */
    static final class FakeRecord extends WALRecord {
        /** */
        private final boolean stop;

        /**
         * @param pos Position.
         */
        FakeRecord(FileWALPointer pos, boolean stop) {
            position(pos);

            this.stop = stop;
        }

        /** {@inheritDoc} */
        @Override public RecordType type() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FileWALPointer position() {
            return (FileWALPointer)super.position();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FakeRecord.class, this, "super", super.toString());
        }
    }
}
