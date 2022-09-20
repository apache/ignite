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

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.SwitchSegmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.prepareSerializerVersionBuffer;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory.LATEST_SERIALIZER_VERSION;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;
import static org.apache.ignite.internal.util.IgniteUtils.findField;
import static org.apache.ignite.internal.util.IgniteUtils.findNonPublicMethod;
import static org.apache.ignite.internal.util.IgniteUtils.jdkVersion;
import static org.apache.ignite.internal.util.IgniteUtils.majorJavaVersion;

/**
 * File handle for one log segment.
 */
@SuppressWarnings("SignalWithoutCorrespondingAwait")
class FileWriteHandleImpl extends AbstractFileHandle implements FileWriteHandle {
    /** Memory mapped buffer fsync operation runner. */
    private static final MMapFSyncer FSYNCER = pickFsyncer();

    /** {@link FileWriteHandleImpl#written} atomic field updater. */
    private static final AtomicLongFieldUpdater<FileWriteHandleImpl> WRITTEN_UPD =
        AtomicLongFieldUpdater.newUpdater(FileWriteHandleImpl.class, "written");

    /** Page size. */
    private static final int PAGE_SIZE = GridUnsafe.pageSize();

    /** Serializer latest version to use. */
    private final int serializerVer =
        IgniteSystemProperties.getInteger(IGNITE_WAL_SERIALIZER_VERSION, LATEST_SERIALIZER_VERSION);

    /** Use mapped byte buffer. */
    private final boolean mmap;

    /** Created on resume logging. */
    private volatile boolean resume;

    /**
     * Position in current file after the end of last written record (incremented after file channel write operation)
     */
    volatile long written;

    /** */
    protected volatile long lastFsyncPos;

    /** Stop guard to provide warranty that only one thread will be successful in calling {@link #close(boolean)}. */
    protected final AtomicBoolean stop = new AtomicBoolean(false);

    /** */
    private final Lock lock = new ReentrantLock();

    /** Condition for timed wait of several threads, see {@link DataStorageConfiguration#getWalFsyncDelayNanos()}. */
    private final Condition fsync = lock.newCondition();

    /**
     * Next segment available condition. Protection from "spurious wakeup" is provided by predicate {@link
     * #fileIO}=<code>null</code>.
     */
    private final Condition nextSegment = lock.newCondition();

    /** Buffer. */
    protected final SegmentedRingByteBuffer buf;

    /** */
    private final WALMode mode;

    /** Fsync delay. */
    private final long fsyncDelay;

    /** Persistence metrics tracker. */
    private final DataStorageMetricsImpl metrics;

    /** WAL segment size in bytes. This is maximum value, actual segments may be shorter. */
    private final long maxWalSegmentSize;

    /** Logger. */
    protected final IgniteLogger log;

    /** */
    private final RecordSerializer serializer;

    /** Context. */
    protected final GridCacheSharedContext cctx;

    /** WAL writer worker. */
    private final FileHandleManagerImpl.WALWriter walWriter;

    /** Switch segment record offset. */
    private int switchSegmentRecordOffset;

    /**
     * @param cctx Context.
     * @param fileIO I/O file interface to use
     * @param serializer Serializer.
     * @param metrics Data storage metrics.
     * @param writer WAL writer.
     * @param pos Initial position.
     * @param mode WAL mode.
     * @param mmap Mmap.
     * @param resume Created on resume logging flag.
     * @param fsyncDelay Fsync delay.
     * @param maxWalSegmentSize Max WAL segment size.
     * @throws IOException If failed.
     */
    FileWriteHandleImpl(
        GridCacheSharedContext cctx, SegmentIO fileIO, SegmentedRingByteBuffer rbuf, RecordSerializer serializer,
        DataStorageMetricsImpl metrics, FileHandleManagerImpl.WALWriter writer, long pos, WALMode mode, boolean mmap,
        boolean resume, long fsyncDelay, long maxWalSegmentSize) throws IOException {
        super(fileIO);
        assert serializer != null;

        this.mmap = mmap;
        this.mode = mode;
        this.fsyncDelay = fsyncDelay;
        this.metrics = metrics;
        this.maxWalSegmentSize = maxWalSegmentSize;
        this.log = cctx.logger(FileWriteHandleImpl.class);
        this.cctx = cctx;
        this.walWriter = writer;
        this.serializer = serializer;
        this.written = pos;
        this.lastFsyncPos = pos;
        this.resume = resume;
        this.buf = rbuf;

        if (!mmap)
            fileIO.position(pos);
    }

    /** {@inheritDoc} */
    @Override public int serializerVersion() {
        return serializer.version();
    }

    /** {@inheritDoc} */
    @Override public void finishResumeLogging() {
        resume = false;
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
     * Write serializer version to current handle.
     */
    @Override public void writeHeader() {
        SegmentedRingByteBuffer.WriteSegment seg = buf.offer(HEADER_RECORD_SIZE);

        assert seg != null && seg.position() > 0;

        prepareSerializerVersionBuffer(getSegmentId(), serializerVer, false, seg.buffer());

            seg.release();
    }

    /**
     * @param rec Record to be added to write queue.
     * @return Pointer or null if roll over to next segment is required or already started by other thread.
     * @throws StorageException If failed.
     * @throws IgniteCheckedException If failed.
     */
    @Override @Nullable public WALPointer addRecord(WALRecord rec) throws StorageException, IgniteCheckedException {
        assert rec.size() > 0 : rec;

//        log.error("TETS | addRecord " + rec.getClass().getSimpleName());

        for (; ; ) {
            checkNode();

            SegmentedRingByteBuffer.WriteSegment seg;

            // Buffer can be in open state in case of resuming with different serializer version.
            if (rec.type() == SWITCH_SEGMENT_RECORD && !resume)
                seg = buf.offerSafe(rec.size());
            else
                seg = buf.offer(rec.size());

            WALPointer ptr = null;

            if (seg != null) {
                try {
                    int pos = (int)(seg.position() - rec.size());

                    ByteBuffer buf = seg.buffer();

                    if (buf == null)
                        return null; // Can not write to this segment, need to switch to the next one.

                    ptr = new WALPointer(getSegmentId(), pos, rec.size());

                    rec.position(ptr);

                    fillBuffer(buf, rec);

                    if (mmap) {
                        // written field must grow only, but segment with greater position can be serialized
                        // earlier than segment with smaller position.
                        while (true) {
                            long written0 = written;

                            if (seg.position() > written0) {
                                if (WRITTEN_UPD.compareAndSet(this, written0, seg.position()))
                                    break;
                            }
                            else
                                break;
                        }
                    }

                    return ptr;
                }
                finally {
                    seg.release();

                    if (mode == WALMode.BACKGROUND && rec instanceof CheckpointRecord)
                        flushOrWait(ptr);
                }
            }
            else
                walWriter.flushAll();
        }
    }

    /**
     * Flush or wait for concurrent flush completion.
     *
     * @param ptr Pointer.
     */
    public void flushOrWait(WALPointer ptr) throws IgniteCheckedException {
        if (ptr != null) {
            // If requested obsolete file index, it must be already flushed by close.
            if (ptr.index() != getSegmentId())
                return;
        }

        flush(ptr);
    }

    /** {@inheritDoc} */
    @Override public void flushAll() throws IgniteCheckedException {
        flush(null);
    }

    /**
     * @param ptr Pointer.
     */
    public void flush(WALPointer ptr) throws IgniteCheckedException {
        if (ptr == null) { // Unconditional flush.
            walWriter.flushAll();

            return;
        }

        assert ptr.index() == getSegmentId() : "Pointer segment idx is not equals to current write segment idx. " +
            "ptr=" + ptr + " segmetntId=" + getSegmentId();

        walWriter.flushBuffer(ptr.fileOffset() + ptr.length());
    }

    /**
     * @param buf Buffer.
     * @param rec WAL record.
     * @throws IgniteCheckedException If failed.
     */
    private void fillBuffer(ByteBuffer buf, WALRecord rec) throws IgniteCheckedException {
        try {
            serializer.writeRecord(rec, buf);
        }
        catch (RuntimeException e) {
            throw new IllegalStateException("Failed to write record: " + rec, e);
        }
    }

    /**
     * Non-blocking check if this pointer needs to be sync'ed.
     *
     * @param ptr WAL pointer to check.
     * @return {@code False} if this pointer has been already sync'ed.
     */
    @Override public boolean needFsync(WALPointer ptr) {
        // If index has changed, it means that the log was rolled over and already sync'ed.
        // If requested position is smaller than last sync'ed, it also means all is good.
        // If position is equal, then our record is the last not synced.
        return getSegmentId() == ptr.index() && lastFsyncPos <= ptr.fileOffset();
    }

    /**
     * @return Pointer to the end of the last written record (probably not fsync-ed).
     */
    @Override public WALPointer position() {
        lock.lock();

        try {
            return new WALPointer(getSegmentId(), (int)written, 0);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param ptr Pointer to sync.
     * @throws StorageException If failed.
     */
    @Override public void fsync(WALPointer ptr) throws StorageException, IgniteCheckedException {
        lock.lock();

        try {
            if (ptr != null) {
                if (!needFsync(ptr))
                    return;

                if (fsyncDelay > 0 && !stop.get()) {
                    // Delay fsync to collect as many updates as possible: trade latency for throughput.
                    U.await(fsync, fsyncDelay, TimeUnit.NANOSECONDS);

                    if (!needFsync(ptr))
                        return;
                }
            }

            flushOrWait(ptr);

            if (stop.get())
                return;

            long lastFsyncPos0 = lastFsyncPos;
            long written0 = written;

            if (lastFsyncPos0 != written0) {
                // Fsync position must be behind.
                assert lastFsyncPos0 < written0 : "lastFsyncPos=" + lastFsyncPos0 + ", written=" + written0;

                boolean metricsEnabled = metrics.metricsEnabled();

                long start = metricsEnabled ? System.nanoTime() : 0;

                if (mmap) {
                    long pos = ptr == null ? -1 : ptr.fileOffset();

                    List<SegmentedRingByteBuffer.ReadSegment> segs = buf.poll(pos);

                    if (segs != null) {
                        assert segs.size() == 1;

                        SegmentedRingByteBuffer.ReadSegment seg = segs.get(0);

                        int off = seg.buffer().position();
                        int len = seg.buffer().limit() - off;

                        fsync((MappedByteBuffer)buf.buf, off, len);

                        seg.release();
                    }
                }
                else
                    walWriter.force();

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
     * @param buf Mapped byte buffer.
     * @param off Offset.
     * @param len Length.
     */
    private static void fsync(MappedByteBuffer buf, int off, int len) throws IgniteCheckedException {
        FSYNCER.fsync(buf, off, len);
    }

    /** {@inheritDoc} */
    @Override public void closeBuffer() {
        buf.close();
    }

    /**
     * @return {@code true} If this thread actually closed the segment.
     * @throws IgniteCheckedException If failed.
     * @throws StorageException If failed.
     */
    @Override public boolean close(boolean rollOver) throws IgniteCheckedException, StorageException {
        if (stop.compareAndSet(false, true)) {
            lock.lock();

            try {
                flushOrWait(null);

                RecordSerializer backwardSerializer = new RecordSerializerFactoryImpl(cctx)
                    .createSerializer(serializerVer);

                SwitchSegmentRecord segmentRecord = new SwitchSegmentRecord();

                int switchSegmentRecSize = backwardSerializer.size(segmentRecord);

                if (rollOver && written + switchSegmentRecSize < maxWalSegmentSize) {
                    segmentRecord.size(switchSegmentRecSize);

                    WALPointer segRecPtr = addRecord(segmentRecord);

                    if (segRecPtr != null) {
                        fsync(segRecPtr);

                        switchSegmentRecordOffset = segRecPtr.fileOffset() + switchSegmentRecSize;
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Not enough space in wal segment to write segment switch");
                    }
                }
                else {
                    if (log.isDebugEnabled()) {
                        log.debug("Not enough space in wal segment to write segment switch, written="
                            + written + ", switchSegmentRecSize=" + switchSegmentRecSize);
                    }
                }

                // Unconditional flush (tail of the buffer)
                flushOrWait(null);

                if (mmap) {
                    List<SegmentedRingByteBuffer.ReadSegment> segs = buf.poll(maxWalSegmentSize);

                    if (segs != null) {
                        assert segs.size() == 1;

                        segs.get(0).release();
                    }
                }

                // Do the final fsync.
                if (mode != WALMode.NONE) {
                    if (mmap)
                        ((MappedByteBuffer)buf.buf).force();
                    else
                        walWriter.force();

                    lastFsyncPos = written;
                }

                if (mmap)
                    U.closeQuiet(fileIO);
                else
                    walWriter.close();

                if (!mmap && !rollOver)
                    buf.free();

                if (log.isDebugEnabled())
                    log.debug("Closed WAL write handle [idx=" + getSegmentId() + "]");

                return true;
            }
            finally {
                if (mmap)
                    buf.free();

                lock.unlock();
            }
        }
        else
            return false;
    }

    /**
     * Signals next segment available to wake up other worker threads waiting for WAL to write.
     */
    @Override public void signalNextAvailable() {
        lock.lock();

        try {
            assert cctx.kernalContext().invalid() ||
                written == lastFsyncPos || mode != WALMode.FSYNC :
                "fsync [written=" + written + ", lastFsync=" + lastFsyncPos + ", idx=" + getSegmentId() + ']';

            fileIO = null;

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
            while (fileIO != null)
                U.awaitQuiet(nextSegment);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return Safely reads current position of the file channel as String. Will return "null" if channel is null.
     */
    public String safePosition() {
        FileIO io = fileIO;

        if (io == null)
            return "null";

        try {
            return String.valueOf(io.position());
        }
        catch (IOException e) {
            return "{Failed to read channel position: " + e.getMessage() + '}';
        }
    }

    /** {@inheritDoc} */
    @Override public int getSwitchSegmentRecordOffset() {
        return switchSegmentRecordOffset;
    }

    /**
     * Interface for performing {@link MappedByteBuffer} fsync.
     */
    private interface MMapFSyncer {
        /** {@link MappedByteBuffer#fd} */
        static final Field fd = findField(MappedByteBuffer.class, "fd");

        /**
         * Performs fsync.
         *
         * @param buf Mmapped byte buffer.
         * @param index Index of the syncing segment in the buffer.
         * @param len Length of the syncing segment part.
         * @throws IgniteCheckedException If failed.
         */
        void fsync(MappedByteBuffer buf, int index, int len) throws IgniteCheckedException;
    }

    /**
     * @return FSyncer suitable for the current JRE.
     */
    private static MMapFSyncer pickFsyncer() {
        int javaVersion = majorJavaVersion(jdkVersion());

        if (javaVersion >= 15)
            return new JDK15FSyncer();

        return new LegacyFSyncer();
    }

    /**
     * Runs fsync operation on JRE15 and higher using MappedMemoryUtils which provides aligned fsync.
     */
    private static class JDK15FSyncer implements MMapFSyncer {
        /** {@link MappedByteBuffer#address}. */
        private static final Field address = findField(MappedByteBuffer.class, "address");

        /**
         * A flag true if this buffer is mapped against non-volatile
         * memory using one of the extended FileChannel.MapMode modes.
         */
        private static final Field isSync = findField(MappedByteBuffer.class, "isSync");

        /**
         * Mapped memory utils class. For compatibility reasons it might only be accessed via
         * {@link Class#forName(String)}.
         */
        private final Class<?> mappedMemoryUtils;

        /**
         * MappedMemoryUtils#force method.
         */
        private final Method force;

        /** Constructor. */
        public JDK15FSyncer() {
            try {
                mappedMemoryUtils = Class.forName("java.nio.MappedMemoryUtils");

                force = findNonPublicMethod(
                    mappedMemoryUtils,
                    "force",
                    FileDescriptor.class,
                    long.class, // Address
                    boolean.class, // Is sync?
                    long.class, // Index
                    long.class // Length
                );
            }
            catch (ClassNotFoundException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void fsync(MappedByteBuffer buf, int index, int len) throws IgniteCheckedException {
            try {
                boolean isSync = (boolean)JDK15FSyncer.isSync.get(buf);
                long address = (long)JDK15FSyncer.address.get(buf);

                assert address % PAGE_SIZE == 0 : "Buffer's address is not aligned: " + address;

                // Don't need to align manually as MappedMemoryUtils does the alignment
                force.invoke(mappedMemoryUtils, fd.get(buf), address, isSync, index, len);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

    /**
     * Runs fsync on pre-java15 JVMs that don't offer a possibility to fsync mapped byte buffers in an aligned way.
     */
    private static class LegacyFSyncer implements MMapFSyncer {
        /** {@link MappedByteBuffer#force0(java.io.FileDescriptor, long, long)}. */
        private static final Method force0 = findNonPublicMethod(
            MappedByteBuffer.class, "force0",
            java.io.FileDescriptor.class, long.class, long.class
        );

        /** {@link MappedByteBuffer#mappingOffset()}. */
        private static final Method mappingOffset = findNonPublicMethod(MappedByteBuffer.class, "mappingOffset");

        /** {@link MappedByteBuffer#mappingAddress(long)}. */
        private static final Method mappingAddress = findNonPublicMethod(
            MappedByteBuffer.class, "mappingAddress", long.class
        );

        /** {@inheritDoc} */
        @Override public void fsync(MappedByteBuffer buf, int index, int len) throws IgniteCheckedException {
            try {
                long mappedOff = (Long)mappingOffset.invoke(buf);

                assert mappedOff == 0 : mappedOff;

                long addr = (Long)mappingAddress.invoke(buf, mappedOff);

                long alignmentDelta = (addr + index) % PAGE_SIZE;

                // Given an alignment delta calculate the largest page aligned address
                // of the mapping less than or equal to the address of the buffer
                // element identified by the index.
                long alignedAddr = (addr + index) - alignmentDelta;

                force0.invoke(buf, fd.get(buf), alignedAddr, len + alignmentDelta);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }
}
