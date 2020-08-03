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
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static java.lang.Long.MAX_VALUE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SEGMENT_SYNC_TIMEOUT;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer.BufferMode.DIRECT;
import static org.apache.ignite.internal.util.IgniteUtils.sleep;

/**
 * Manager for {@link FileWriteHandleImpl}.
 */
public class FileHandleManagerImpl implements FileHandleManager {
    /** Default wal segment sync timeout. */
    private static final long DFLT_WAL_SEGMENT_SYNC_TIMEOUT = 500L;

    /** WAL writer worker. */
    private final WALWriter walWriter;

    /** Wal segment sync worker. */
    private final WalSegmentSyncer walSegmentSyncWorker;

    /** Context. */
    protected final GridCacheSharedContext cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final WALMode mode;

    /** Persistence metrics tracker. */
    private final DataStorageMetricsImpl metrics;

    /** Use mapped byte buffer. */
    private final boolean mmap;

    /** */
    private final RecordSerializer serializer;

    /** Current handle supplier. */
    private final Supplier<FileWriteHandle> currentHandleSupplier;

    /** WAL buffer size. */
    private final int walBufferSize;

    /** WAL segment size in bytes. . This is maximum value, actual segments may be shorter. */
    private final long maxWalSegmentSize;

    /** Fsync delay. */
    private final long fsyncDelay;

    /**
     * @param cctx Context.
     * @param metrics Data storage metrics.
     * @param mmap Mmap.
     * @param serializer Serializer.
     * @param currentHandleSupplier Current handle supplier.
     * @param mode WAL mode.
     * @param walBufferSize WAL buffer size.
     * @param maxWalSegmentSize Max WAL segment size.
     * @param fsyncDelay Fsync delay.
     */
    public FileHandleManagerImpl(
        GridCacheSharedContext cctx,
        DataStorageMetricsImpl metrics,
        boolean mmap,
        RecordSerializer serializer,
        Supplier<FileWriteHandle> currentHandleSupplier,
        WALMode mode,
        int walBufferSize,
        long maxWalSegmentSize,
        long fsyncDelay
    ) {
        this.cctx = cctx;
        log = cctx.logger(FileHandleManagerImpl.class);
        this.mode = mode;
        this.metrics = metrics;
        this.mmap = mmap;
        this.serializer = serializer;
        this.currentHandleSupplier = currentHandleSupplier;
        this.walBufferSize = walBufferSize;
        this.maxWalSegmentSize = maxWalSegmentSize;
        this.fsyncDelay = fsyncDelay;
        walWriter = new WALWriter(log);

        if (mode != WALMode.NONE && mode != WALMode.FSYNC) {
            walSegmentSyncWorker = new WalSegmentSyncer(
                cctx.igniteInstanceName(),
                cctx.kernalContext().log(WalSegmentSyncer.class)
            );

            if (log.isInfoEnabled())
                log.info("Initialized write-ahead log manager [mode=" + mode + ']');
        }
        else {
            U.quietAndWarn(log, "Initialized write-ahead log manager in NONE mode, persisted data may be lost in " +
                "a case of unexpected node failure. Make sure to deactivate the cluster before shutdown.");

            walSegmentSyncWorker = null;
        }
    }

    /** {@inheritDoc} */
    @Override public FileWriteHandle initHandle(
        SegmentIO fileIO,
        long position,
        RecordSerializer serializer
    ) throws IOException {
        SegmentedRingByteBuffer rbuf;

        if (mmap) {
            MappedByteBuffer buf = fileIO.map((int)maxWalSegmentSize);

            rbuf = new SegmentedRingByteBuffer(buf, metrics);
        }
        else
            rbuf = new SegmentedRingByteBuffer(walBufferSize, maxWalSegmentSize, DIRECT, metrics);

        rbuf.init(position);

        return new FileWriteHandleImpl(
            cctx, fileIO, rbuf, serializer, metrics, walWriter, position,
            mode, mmap, true, fsyncDelay, maxWalSegmentSize
        );
    }

    /** {@inheritDoc} */
    @Override public FileWriteHandle nextHandle(SegmentIO fileIO, RecordSerializer serializer) throws IOException {
        SegmentedRingByteBuffer rbuf;

        if (mmap) {
            MappedByteBuffer buf = fileIO.map((int)maxWalSegmentSize);

            rbuf = new SegmentedRingByteBuffer(buf, metrics);
        }
        else
            rbuf = currentHandle().buf.reset();

        try {
            return new FileWriteHandleImpl(
                cctx, fileIO, rbuf, serializer, metrics, walWriter, 0,
                mode, mmap, false, fsyncDelay, maxWalSegmentSize
            );
        }
        catch (ClosedByInterruptException e) {
            if (rbuf != null)
                rbuf.free();
        }

        return null;
    }

    /**
     * @return Current handle.
     */
    private FileWriteHandleImpl currentHandle() {
        return (FileWriteHandleImpl)currentHandleSupplier.get();
    }

    /** {@inheritDoc} */
    @Override public void onDeactivate() throws IgniteCheckedException {
        FileWriteHandleImpl currHnd = currentHandle();

        try {
            if (mode == WALMode.BACKGROUND) {
                if (currHnd != null)
                    currHnd.flush(null);
            }

            if (currHnd != null)
                currHnd.close(false);
        }
        finally {
            if (walSegmentSyncWorker != null)
                walSegmentSyncWorker.shutdown();

            walWriter.shutdown();
        }
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging() {
        if (!mmap)
            walWriter.restart();

        if (cctx.kernalContext().clientNode())
            return;

        if (walSegmentSyncWorker != null)
            walSegmentSyncWorker.restart();
    }

    /** {@inheritDoc} */
    @Override public WALPointer flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return null;

        FileWriteHandleImpl cur = currentHandle();

        // WAL manager was not started (client node).
        if (cur == null)
            return null;

        FileWALPointer filePtr;

        if (ptr == null) {
            long pos = cur.buf.tail();

            filePtr = new FileWALPointer(cur.getSegmentId(), (int)pos, 0);
        }
        else
            filePtr = (FileWALPointer)ptr;

        if (mode == LOG_ONLY)
            cur.flushOrWait(filePtr);

        if (!explicitFsync && mode != WALMode.FSYNC)
            return filePtr; // No need to sync in LOG_ONLY or BACKGROUND unless explicit fsync is required.

        // No need to sync if was rolled over.
        if (!cur.needFsync(filePtr))
            return filePtr;

        cur.fsync(filePtr);

        return filePtr;
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
     * WAL writer worker.
     */
    public class WALWriter extends GridWorker {
        /** Unconditional flush. */
        private static final long UNCONDITIONAL_FLUSH = -1L;

        /** File close. */
        private static final long FILE_CLOSE = -2L;

        /** File force. */
        private static final long FILE_FORCE = -3L;

        /** Err. */
        private volatile Throwable err;

        //TODO: replace with GC free data structure.
        /** Parked threads. */
        final Map<Thread, Long> waiters = new ConcurrentHashMap<>();

        /**
         * Default constructor.
         *
         * @param log Logger.
         */
        WALWriter(IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-write-worker%" + cctx.igniteInstanceName(), log,
                cctx.kernalContext().workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            Throwable err = null;

            try {
                while (!isCancelled()) {
                    onIdle();

                    while (waiters.isEmpty()) {
                        if (!isCancelled()) {
                            blockingSectionBegin();

                            try {
                                LockSupport.park();
                            }
                            finally {
                                blockingSectionEnd();
                            }
                        }
                        else {
                            unparkWaiters(MAX_VALUE);

                            return;
                        }
                    }

                    Long pos = null;

                    for (Long val : waiters.values()) {
                        if (val > Long.MIN_VALUE)
                            pos = val;
                    }

                    updateHeartbeat();

                    if (pos == null)
                        continue;
                    else if (pos < UNCONDITIONAL_FLUSH) {
                        try {
                            assert pos == FILE_CLOSE || pos == FILE_FORCE : pos;

                            if (pos == FILE_CLOSE)
                                currentHandle().fileIO.close();
                            else if (pos == FILE_FORCE)
                                currentHandle().fileIO.force();
                        }
                        catch (IOException e) {
                            log.error("Exception in WAL writer thread: ", e);

                            err = e;

                            unparkWaiters(MAX_VALUE);

                            return;
                        }

                        unparkWaiters(pos);
                    }

                    updateHeartbeat();

                    List<SegmentedRingByteBuffer.ReadSegment> segs = currentHandle().buf.poll(pos);

                    if (segs == null) {
                        unparkWaiters(pos);

                        continue;
                    }

                    for (int i = 0; i < segs.size(); i++) {
                        SegmentedRingByteBuffer.ReadSegment seg = segs.get(i);

                        updateHeartbeat();

                        try {
                            writeBuffer(seg.position(), seg.buffer());
                        }
                        catch (Throwable e) {
                            log.error("Exception in WAL writer thread:", e);

                            err = e;
                        }
                        finally {
                            seg.release();

                            boolean unparkAll = (pos == UNCONDITIONAL_FLUSH || pos == FILE_CLOSE) || err != null;

                            long p = unparkAll ? MAX_VALUE : currentHandle().written;

                            unparkWaiters(p);
                        }
                    }
                }
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                this.err = err;

                unparkWaiters(MAX_VALUE);

                if (err == null && !isCancelled)
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * Shutdowns thread.
         */
        private void shutdown() throws IgniteInterruptedCheckedException {
            U.cancel(this);

            Thread runner = runner();

            if (runner != null) {
                LockSupport.unpark(runner);

                U.join(runner);
            }

            assert walWriter.runner() == null : "WALWriter should be stopped.";
        }

        /**
         * Unparks waiting threads.
         *
         * @param pos Pos.
         */
        private void unparkWaiters(long pos) {
            assert pos > Long.MIN_VALUE : pos;

            for (Map.Entry<Thread, Long> e : waiters.entrySet()) {
                Long val = e.getValue();

                if (val <= pos) {
                    if (val != Long.MIN_VALUE)
                        waiters.put(e.getKey(), Long.MIN_VALUE);

                    LockSupport.unpark(e.getKey());
                }
            }
        }

        /**
         * Forces all made changes to the file.
         */
        void force() throws IgniteCheckedException {
            flushBuffer(FILE_FORCE);
        }

        /**
         * Closes file.
         */
        void close() throws IgniteCheckedException {
            flushBuffer(FILE_CLOSE);
        }

        /**
         * Flushes all data from the buffer.
         */
        void flushAll() throws IgniteCheckedException {
            flushBuffer(UNCONDITIONAL_FLUSH);
        }

        /**
         * @param expPos Expected position.
         */
        void flushBuffer(long expPos) throws IgniteCheckedException {
            if (mmap)
                return;

            Throwable err = walWriter.err;

            if (err != null)
                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));

            if (expPos == UNCONDITIONAL_FLUSH)
                expPos = (currentHandle().buf.tail());

            Thread t = Thread.currentThread();

            waiters.put(t, expPos);

            LockSupport.unpark(walWriter.runner());

            while (true) {
                Long val = waiters.get(t);

                assert val != null : "Only this thread can remove thread from waiters";

                if (val == Long.MIN_VALUE) {
                    waiters.remove(t);

                    Throwable walWriterError = walWriter.err;

                    if (walWriterError != null)
                        throw new IgniteCheckedException("Flush buffer failed.", walWriterError);

                    return;
                }
                else
                    LockSupport.park();
            }
        }

        /**
         * @param pos Position in file to start write from. May be checked against actual position to wait previous
         * writes to complete.
         * @param buf Buffer to write to file.
         * @throws StorageException If failed.
         * @throws IgniteCheckedException If failed.
         */
        private void writeBuffer(long pos, ByteBuffer buf) throws StorageException, IgniteCheckedException {
            FileWriteHandleImpl hdl = currentHandle();

            assert hdl.fileIO != null : "Writing to a closed segment.";

            checkNode();

            long lastLogged = U.currentTimeMillis();

            long logBackoff = 2_000;

            // If we were too fast, need to wait previous writes to complete.
            while (hdl.written != pos) {
                assert hdl.written < pos : "written = " + hdl.written + ", pos = " + pos; // No one can write further than we are now.

                // Permutation occurred between blocks write operations.
                // Order of acquiring lock is not the same as order of write.
                long now = U.currentTimeMillis();

                if (now - lastLogged >= logBackoff) {
                    if (logBackoff < 60 * 60_000)
                        logBackoff *= 2;

                    U.warn(log, "Still waiting for a concurrent write to complete [written=" + hdl.written +
                        ", pos=" + pos + ", lastFsyncPos=" + hdl.lastFsyncPos + ", stop=" + hdl.stop.get() +
                        ", actualPos=" + hdl.safePosition() + ']');

                    lastLogged = now;
                }

                checkNode();
            }

            // Do the write.
            int size = buf.remaining();

            assert size > 0 : size;

            try {
                assert hdl.written == hdl.fileIO.position();

                hdl.written += hdl.fileIO.writeFully(buf);

                metrics.onWalBytesWritten(size);

                assert hdl.written == hdl.fileIO.position();
            }
            catch (IOException e) {
                err = e;

                StorageException se = new StorageException("Failed to write buffer.", e);

                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, se));

                throw se;
            }
        }

        /**
         * Restart worker in IgniteThread.
         */
        public void restart() {
            assert runner() == null : "WALWriter is still running.";

            isCancelled = false;

            new IgniteThread(this).start();
        }
    }

    /**
     * Syncs WAL segment file.
     */
    private class WalSegmentSyncer extends GridWorker {
        /** Sync timeout. */
        private final long syncTimeout;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param log Logger.
         */
        private WalSegmentSyncer(String igniteInstanceName, IgniteLogger log) {
            super(igniteInstanceName, "wal-segment-syncer", log);

            syncTimeout = Math.max(IgniteSystemProperties.getLong(IGNITE_WAL_SEGMENT_SYNC_TIMEOUT,
                DFLT_WAL_SEGMENT_SYNC_TIMEOUT), 100L);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                sleep(syncTimeout);

                try {
                    flush(null, true);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Exception when flushing WAL.", e);
                }
            }
        }

        /** Shutted down the worker. */
        private void shutdown() {
            synchronized (this) {
                U.cancel(this);
            }

            U.join(this, log);
        }

        /**
         * Restart worker in IgniteThread.
         */
        public void restart() {
            assert runner() == null : "WalSegmentSyncer is running.";

            isCancelled = false;

            new IgniteThread(walSegmentSyncWorker).start();
        }
    }

}
