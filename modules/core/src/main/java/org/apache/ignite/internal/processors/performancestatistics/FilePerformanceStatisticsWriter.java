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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;

/**
 * Performance statistics collector based on logging to a file.
 * <p>
 * Each node collects statistics to a file placed under {@link #PERFORMANCE_STAT_DIR}.
 * <p>
 * <b>Note:</b> Start again will erase previous performance statistics files.
 * <p>
 * To iterate over records use {@link FilePerformanceStatisticsReader}.
 */
public class FilePerformanceStatisticsWriter {
    /** Default maximum file size in bytes. Performance statistics will be stopped when the size exceeded. */
    public static final long DFLT_FILE_MAX_SIZE = 32 * U.GB;

    /** Default off heap buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = (int)(32 * U.MB);

    /** Default minimal batch size to flush in bytes. */
    public static final int DFLT_FLUSH_SIZE = (int)(8 * U.MB);

    /** Directory to store performance statistics files. Placed under Ignite work directory. */
    public static final String PERFORMANCE_STAT_DIR = "performanceStatistics";

    /** Factory to provide I/O interface. */
    private final FileIOFactory fileIoFactory = new RandomAccessFileIOFactory();

    /** Performance statistics enabled flag. */
    private volatile boolean enabled;

    /** Performance statistics file writer worker. */
    @Nullable private volatile FileWriter fileWriter;

    /** Performance statistics file I/O. */
    @Nullable private volatile FileIO fileIo;

    /** File write buffer. */
    @Nullable private volatile SegmentedRingByteBuffer ringByteBuffer;

    /** Size of ready for flushing bytes. */
    private final AtomicInteger readyForFlushSize = new AtomicInteger();

    /** {@code True} if the small buffer warning message logged. */
    private final AtomicBoolean smallBufLogged = new AtomicBoolean();

    /** {@code True} if worker stopped due to maximum file size reached. */
    private final AtomicBoolean stopByMaxSize = new AtomicBoolean();

    /** Hashcodes of cached strings. */
    private final ConcurrentSkipListSet<Integer> cachedStrings = new ConcurrentSkipListSet<>();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** @param ctx Kernal context. */
    public FilePerformanceStatisticsWriter(GridKernalContext ctx) {
        log = ctx.log(getClass());

        this.ctx = ctx;
    }

    /** Starts collecting performance statistics. */
    public void start() {
        synchronized (this) {
            if (enabled)
                return;

            enabled = true;

            try {
                File file = statisticsFile(ctx);

                U.delete(file);

                fileIo = fileIoFactory.create(file);

                ringByteBuffer = new SegmentedRingByteBuffer(DFLT_BUFFER_SIZE, DFLT_FILE_MAX_SIZE,
                    SegmentedRingByteBuffer.BufferMode.DIRECT);

                ringByteBuffer.init(0);

                fileWriter = new FileWriter(ctx, log);

                new IgniteThread(fileWriter).start();

                log.info("Performance statistics writer started [file=" + file.getAbsolutePath() + ']');
            }
            catch (IOException | IgniteCheckedException e) {
                log.error("Failed to start performance statistics writer.", e);

                stopStatistics();

                throw new IgniteException("Failed to start performance statistics writer.", e);
            }
        }
    }

    /** Stops collecting performance statistics. */
    public void stop() {
        synchronized (this) {
            if (!enabled)
                return;

            enabled = false;

            FileWriter fileWriter = this.fileWriter;

            SegmentedRingByteBuffer buf = ringByteBuffer;

            // Stop write new data.
            if (buf != null)
                buf.close();

            // Make sure that all buffer's producers released to safe deallocate memory.
            if (fileWriter != null)
                U.awaitForWorkersStop(Collections.singleton(fileWriter), true, log);

            if (buf != null)
                buf.free();

            U.closeQuiet(fileIo);

            readyForFlushSize.set(0);
            smallBufLogged.set(false);
            stopByMaxSize.set(false);
            cachedStrings.clear();

            log.info("Performance statistics writer stopped.");
        }
    }

    /**
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     */
    public void cacheOperation(OperationType type, int cacheId, long startTime, long duration) {
        doWrite(type,
            () -> 4 + 8 + 8,
            buf -> {
                buf.putInt(cacheId);
                buf.putLong(startTime);
                buf.putLong(duration);
            });
    }

    /**
     * @param cacheIds Cache IDs.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param commited {@code True} if commited.
     */
    public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commited) {
        doWrite(commited ? TX_COMMIT : TX_ROLLBACK,
            () -> 4 + cacheIds.size() * 4 + 8 + 8,
            buf -> {
                buf.putInt(cacheIds.size());

                GridIntIterator iter = cacheIds.iterator();

                while (iter.hasNext())
                    buf.putInt(iter.next());

                buf.putLong(startTime);
                buf.putLong(duration);
            });
    }

    /**
     * @param type Cache query type.
     * @param text Query text in case of SQL query. Cache name in case of SCAN query.
     * @param id Query id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param success Success flag.
     */
    public void query(GridCacheQueryType type, String text, long id, long startTime, long duration, boolean success) {
        boolean needWriteStr = !stringCached(text);
        byte[] strBytes = needWriteStr ? text.getBytes() : null;

        doWrite(QUERY, () -> {
            int size = 1 + 1 + 4 + 8 + 8 + 8 + 1;

            if (needWriteStr)
                size += 4 + strBytes.length;

            return size;
        }, buf -> {
            buf.put((byte)type.ordinal());
            buf.put(needWriteStr ? (byte)1 : 0);
            buf.putInt(text.hashCode());

            if (needWriteStr) {
                buf.putInt(strBytes.length);
                buf.put(strBytes);
            }

            buf.putLong(id);
            buf.putLong(startTime);
            buf.putLong(duration);
            buf.put(success ? (byte)1 : 0);
        });
    }

    /**
     * @param type Cache query type.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param logicalReads Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads, long physicalReads) {
        doWrite(QUERY_READS,
            () -> 1 + 16 + 8 + 8 + 8,
            buf -> {
                buf.put((byte)type.ordinal());
                writeUuid(buf, queryNodeId);
                buf.putLong(id);
                buf.putLong(logicalReads);
                buf.putLong(physicalReads);
            });
    }

    /**
     * @param sesId Session id.
     * @param taskName Task name.
     * @param startTime Start time in milliseconds.
     * @param duration Duration.
     * @param affPartId Affinity partition id.
     */
    public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        boolean needWriteStr = !stringCached(taskName);
        byte[] strBytes = needWriteStr ? taskName.getBytes() : null;

        doWrite(TASK, () -> {
            int size = 24 + 1 + 4 + 8 + 8 + 4;

            if (needWriteStr)
                size += 4 + strBytes.length;

            return size;
        }, buf -> {
            writeIgniteUuid(buf, sesId);
            buf.put(needWriteStr ? (byte)1 : 0);
            buf.putInt(taskName.hashCode());

            if (needWriteStr) {
                buf.putInt(strBytes.length);
                buf.put(strBytes);
            }

            buf.putLong(startTime);
            buf.putLong(duration);
            buf.putInt(affPartId);
        });
    }

    /**
     * @param sesId Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime Start time in milliseconds.
     * @param duration Job execution time.
     * @param timedOut {@code True} if job is timed out.
     */
    public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        doWrite(JOB,
            () -> 24 + 8 + 8 + 8 + 1,
            buf -> {
                writeIgniteUuid(buf, sesId);
                buf.putLong(queuedTime);
                buf.putLong(startTime);
                buf.putLong(duration);
                buf.put(timedOut ? (byte)1 : 0);
            });
    }

    /**
     * @param op Operation type.
     * @param sizeSupplier Record size supplier.
     * @param writer Record writer.
     */
    private void doWrite(OperationType op, IntSupplier sizeSupplier, Consumer<ByteBuffer> writer) {
        FileWriter fileWriter = this.fileWriter;

        // Writer stopping.
        if (fileWriter == null)
            return;

        int size = sizeSupplier.getAsInt() + /*type*/ 1;

        SegmentedRingByteBuffer ringBuf = ringByteBuffer;

        // Starting.
        if (ringBuf == null)
            return;

        SegmentedRingByteBuffer.WriteSegment seg = ringBuf.offer(size);

        if (seg == null) {
            if (smallBufLogged.compareAndSet(false, true)) {
                log.warning("The performance statistics in-memory buffer size is too small. Some operations " +
                    "will not be logged.");
            }

            return;
        }

        // Ring buffer closed (writer stopping) or maximum size reached.
        if (seg.buffer() == null) {
            seg.release();

            if (!fileWriter.isCancelled() && stopByMaxSize.compareAndSet(false, true)) {
                stopStatistics();

                log.warning("The performance statistics file maximum size is reached. " +
                    "Performance statistics collecting will be stopped.");
            }

            return;
        }

        ByteBuffer buf = seg.buffer();

        buf.put((byte)op.ordinal());

        writer.accept(buf);

        seg.release();

        int bufCnt = readyForFlushSize.get() / DFLT_FLUSH_SIZE;

        if (readyForFlushSize.addAndGet(size) / DFLT_FLUSH_SIZE > bufCnt)
            fileWriter.wakeUp();
    }

    /** @return {@code True} if string is cached. {@code False} if need write string.  */
    private boolean stringCached(String str) {
        boolean cached = cachedStrings.contains(str.hashCode());

        if (!cached)
            cachedStrings.add(str.hashCode());

        return cached;
    }

    /** @return Performance statistics file. */
    private static File statisticsFile(GridKernalContext ctx) throws IgniteCheckedException {
        String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

        File fileDir = U.resolveWorkDirectory(igniteWorkDir, PERFORMANCE_STAT_DIR, false);

        return new File(fileDir, "node-" + ctx.localNodeId() + ".prf");
    }

    /** Writes {@link UUID} to buffer. */
    private static void writeUuid(ByteBuffer buf, UUID uuid) {
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
    }

    /** Writes {@link IgniteUuid} to buffer. */
    private static void writeIgniteUuid(ByteBuffer buf, IgniteUuid uuid) {
        buf.putLong(uuid.globalId().getMostSignificantBits());
        buf.putLong(uuid.globalId().getLeastSignificantBits());
        buf.putLong(uuid.localId());
    }

    /** @return {@code True} if collecting performance statistics enabled. */
    public boolean enabled() {
        return enabled;
    }

    /** Stops collecting statistics in the cluster. */
    void stopStatistics() {
        try {
            ctx.performanceStatistics().stopCollectStatistics();
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to stop performance statistics.", e);
        }
    }

    /** Worker to write to performance statistics file. */
    private class FileWriter extends GridWorker {
        /**
         * @param ctx Kernal context.
         * @param log Logger.
         */
        FileWriter(GridKernalContext ctx, IgniteLogger log) {
            super(ctx.igniteInstanceName(), "performance-statistics-writer%" + ctx.igniteInstanceName(), log,
                ctx.workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                long bufCnt = 0;

                while (!isCancelled()) {
                    blockingSectionBegin();

                    try {
                        synchronized (this) {
                            if (bufCnt == readyForFlushSize.get() / DFLT_FLUSH_SIZE)
                                wait();
                        }
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    flushBuffer();

                    bufCnt = readyForFlushSize.get() / DFLT_FLUSH_SIZE;
                }
            }
            finally {
                // Make sure that all producers released their buffers to safe deallocate memory.
                flushBuffer();
            }
        }

        /** Flushes to disk available bytes from the ring buffer. */
        private void flushBuffer() {
            List<SegmentedRingByteBuffer.ReadSegment> segs = ringByteBuffer.poll();

            if (segs == null)
                return;

            try {
                for (SegmentedRingByteBuffer.ReadSegment seg : segs) {
                    updateHeartbeat();

                    try {
                        fileIo.writeFully(seg.buffer());
                    }
                    finally {
                        seg.release();
                    }
                }

                fileIo.force();
            } catch (IOException e) {
                log.error("Unable to write to file. Performance statistics collecting will be stopped.", e);

                if (!isCancelled())
                    stopStatistics();
            }
        }

        /** Wake up worker to start writing data to the file. */
        void wakeUp() {
            synchronized (this) {
                notify();
            }
        }
    }
}
