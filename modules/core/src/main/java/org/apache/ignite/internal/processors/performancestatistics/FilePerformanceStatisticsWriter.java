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
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CHECKPOINT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.PAGES_WRITE_THROTTLE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_PROPERTY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_ROWS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheStartRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.checkpointRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.pagesWriteThrottleRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryPropertyRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryReadsRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryRowsRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.taskRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.transactionRecordSize;

/**
 * Performance statistics writer based on logging to a file.
 * <p>
 * Each node collects statistics to a file placed under {@link #PERF_STAT_DIR}.
 * <p>
 * To iterate over records use {@link FilePerformanceStatisticsReader}.
 */
public class FilePerformanceStatisticsWriter extends AbstractFilePerformanceStatisticsWriter {
    /** File writer thread name. */
    static final String WRITER_THREAD_NAME = "performance-statistics-writer";

    /** Performance statistics file writer worker. */
    private final FileWriter fileWriter;

    /** File writer thread started flag. */
    private boolean started;

    /** File write buffer. */
    private final SegmentedRingByteBuffer ringByteBuf;

    /** Count of written to buffer bytes. */
    private final AtomicInteger writtenToBuf = new AtomicInteger();

    /** {@code True} if the small buffer warning message logged. */
    private final AtomicBoolean smallBufLogged = new AtomicBoolean();

    /** {@code True} if worker stopped due to maximum file size reached. */
    private final AtomicBoolean stopByMaxSize = new AtomicBoolean();

    /** Logger. */
    private final IgniteLogger log;

    /** @param ctx Kernal context. */
    public FilePerformanceStatisticsWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
        super(ctx, "node-" + ctx.localNodeId());

        log = ctx.log(getClass());

        log.info("Performance statistics file created [file=" + file.getAbsolutePath() + ']');

        ringByteBuf = new SegmentedRingByteBuffer(bufSize, fileMaxSize, SegmentedRingByteBuffer.BufferMode.DIRECT);

        fileWriter = new FileWriter(ctx, log);

        doWrite(OperationType.VERSION, OperationType.versionRecordSize(), buf -> buf.putShort(FILE_FORMAT_VERSION));
    }

    /** Starts collecting performance statistics. */
    @Override public synchronized void start() {
        assert !started;

        new IgniteThread(fileWriter).start();

        started = true;
    }

    /** Stops collecting performance statistics. */
    @Override public synchronized void stop() {
        assert started;

        // Stop accepting new records.
        ringByteBuf.close();

        U.awaitForWorkersStop(Collections.singleton(fileWriter), true, log);

        // Make sure that all producers released their buffers to safe deallocate memory (in case of worker
        // stopped abnormally).
        ringByteBuf.poll();

        ringByteBuf.free();

        try {
            fileIo.force();
        }
        catch (IOException e) {
            log.warning("Failed to fsync the performance statistics file.", e);
        }

        cleanup();

        started = false;
    }

    /**
     * @param cacheId Cache id.
     * @param name Cache name.
     */
    public void cacheStart(int cacheId, String name) {
        boolean cached = cacheIfPossible(name);

        doWrite(CACHE_START, cacheStartRecordSize(cached ? 0 : name.getBytes().length, cached), buf -> {
            writeString(buf, name, cached);
            buf.putInt(cacheId);
        });
    }

    /**
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     */
    public void cacheOperation(OperationType type, int cacheId, long startTime, long duration) {
        doWrite(type, cacheRecordSize(), buf -> {
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
        doWrite(commited ? TX_COMMIT : TX_ROLLBACK, transactionRecordSize(cacheIds.size()), buf -> {
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
        boolean cached = cacheIfPossible(text);

        doWrite(QUERY, queryRecordSize(cached ? 0 : text.getBytes().length, cached), buf -> {
            writeString(buf, text, cached);
            buf.put((byte)type.ordinal());
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
        doWrite(QUERY_READS, queryReadsRecordSize(), buf -> {
            buf.put((byte)type.ordinal());
            writeUuid(buf, queryNodeId);
            buf.putLong(id);
            buf.putLong(logicalReads);
            buf.putLong(physicalReads);
        });
    }

    /**
     * @param type Cache query type.
     * @param qryNodeId Originating node id.
     * @param id Query id.
     * @param action Action with rows.
     * @param rows Number of rows.
     */
    public void queryRows(GridCacheQueryType type, UUID qryNodeId, long id, String action, long rows) {
        boolean cached = cacheIfPossible(action);

        doWrite(QUERY_ROWS, queryRowsRecordSize(cached ? 0 : action.getBytes().length, cached), buf -> {
            writeString(buf, action, cached);
            buf.put((byte)type.ordinal());
            writeUuid(buf, qryNodeId);
            buf.putLong(id);
            buf.putLong(rows);
        });
    }

    /**
     * @param type Cache query type.
     * @param qryNodeId Originating node id.
     * @param id Query id.
     * @param name Query property name.
     * @param val Query property value.
     */
    public void queryProperty(GridCacheQueryType type, UUID qryNodeId, long id, String name, String val) {
        if (val == null)
            return;

        boolean cachedName = cacheIfPossible(name);
        boolean cachedVal = cacheIfPossible(val);

        doWrite(QUERY_PROPERTY,
            queryPropertyRecordSize(cachedName ? 0 : name.getBytes().length, cachedName, cachedVal ? 0 : val.getBytes().length, cachedVal),
            buf -> {
                writeString(buf, name, cachedName);
                writeString(buf, val, cachedVal);
                buf.put((byte)type.ordinal());
                writeUuid(buf, qryNodeId);
                buf.putLong(id);
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
        boolean cached = cacheIfPossible(taskName);

        doWrite(TASK, taskRecordSize(cached ? 0 : taskName.getBytes().length, cached), buf -> {
            writeString(buf, taskName, cached);
            writeIgniteUuid(buf, sesId);
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
        doWrite(JOB, jobRecordSize(), buf -> {
            writeIgniteUuid(buf, sesId);
            buf.putLong(queuedTime);
            buf.putLong(startTime);
            buf.putLong(duration);
            buf.put(timedOut ? (byte)1 : 0);
        });
    }

    /** @return Performance statistics file. */
    File file() {
        return file;
    }

    /**
     * @param beforeLockDuration Before lock duration.
     * @param lockWaitDuration Lock wait duration.
     * @param listenersExecDuration Listeners execute duration.
     * @param markDuration Mark duration.
     * @param lockHoldDuration Lock hold duration.
     * @param pagesWriteDuration Pages write duration.
     * @param fsyncDuration Fsync duration.
     * @param walCpRecordFsyncDuration Wal cp record fsync duration.
     * @param writeCpEntryDuration Write checkpoint entry duration.
     * @param splitAndSortCpPagesDuration Split and sort cp pages duration.
     * @param recoveryDataWriteDuration Recovery data write duration in milliseconds.
     * @param totalDuration Total duration in milliseconds.
     * @param cpStartTime Checkpoint start time in milliseconds.
     * @param pagesSize Pages size.
     * @param dataPagesWritten Data pages written.
     * @param cowPagesWritten Cow pages written.
     */
    public void checkpoint(
        long beforeLockDuration,
        long lockWaitDuration,
        long listenersExecDuration,
        long markDuration,
        long lockHoldDuration,
        long pagesWriteDuration,
        long fsyncDuration,
        long walCpRecordFsyncDuration,
        long writeCpEntryDuration,
        long splitAndSortCpPagesDuration,
        long recoveryDataWriteDuration,
        long totalDuration,
        long cpStartTime,
        int pagesSize,
        int dataPagesWritten,
        int cowPagesWritten
    ) {
        doWrite(CHECKPOINT, checkpointRecordSize(), buf -> {
            buf.putLong(beforeLockDuration);
            buf.putLong(lockWaitDuration);
            buf.putLong(listenersExecDuration);
            buf.putLong(markDuration);
            buf.putLong(lockHoldDuration);
            buf.putLong(pagesWriteDuration);
            buf.putLong(fsyncDuration);
            buf.putLong(walCpRecordFsyncDuration);
            buf.putLong(writeCpEntryDuration);
            buf.putLong(splitAndSortCpPagesDuration);
            buf.putLong(recoveryDataWriteDuration);
            buf.putLong(totalDuration);
            buf.putLong(cpStartTime);
            buf.putInt(pagesSize);
            buf.putInt(dataPagesWritten);
            buf.putInt(cowPagesWritten);
        });
    }

    /**
     * @param endTime End time in milliseconds.
     * @param duration Duration in milliseconds.
     */
    public void pagesWriteThrottle(long endTime, long duration) {
        doWrite(PAGES_WRITE_THROTTLE, pagesWriteThrottleRecordSize(), buf -> {
            buf.putLong(endTime);
            buf.putLong(duration);
        });
    }

    /**
     * @param op Operation type.
     * @param recSize Record size.
     * @param writer Record writer.
     */
    private void doWrite(OperationType op, int recSize, Consumer<ByteBuffer> writer) {
        int size = recSize + /*type*/ 1;

        SegmentedRingByteBuffer.WriteSegment seg = ringByteBuf.offer(size);

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

            if (!fileWriter.isCancelled() && stopByMaxSize.compareAndSet(false, true))
                log.warning("The performance statistics file maximum size is reached.");

            return;
        }

        ByteBuffer buf = seg.buffer();

        buf.put(op.id());

        writer.accept(buf);

        seg.release();

        int bufCnt = writtenToBuf.get() / flushSize;

        if (writtenToBuf.addAndGet(size) / flushSize > bufCnt) {
            // Wake up worker to start writing data to the file.
            synchronized (fileWriter) {
                fileWriter.notify();
            }
        }
    }

    /** {@inheritDoc} */
    String fileAbsolutePath() {
        return "";
    }

    /** Worker to write to performance statistics file. */
    private class FileWriter extends GridWorker {
        /**
         * @param ctx Kernal context.
         * @param log Logger.
         */
        FileWriter(GridKernalContext ctx, IgniteLogger log) {
            super(ctx.igniteInstanceName(), WRITER_THREAD_NAME, log, ctx.workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                long writtenToFile = 0;

                while (!isCancelled()) {
                    blockingSectionBegin();

                    try {
                        synchronized (this) {
                            if (writtenToFile / flushSize == writtenToBuf.get() / flushSize)
                                wait();
                        }
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    writtenToFile += flush();
                }

                flush();
            }
            catch (InterruptedException e) {
                try {
                    flush();
                }
                catch (IOException ignored) {
                    // No-op.
                }
            }
            catch (ClosedByInterruptException ignored) {
                // No-op.
            }
            catch (IOException e) {
                log.error("Unable to write to the performance statistics file.", e);
            }
        }

        /**
         * Flushes to disk available bytes from the ring buffer.
         *
         * @return Count of written bytes.
         */
        private int flush() throws IOException {
            List<SegmentedRingByteBuffer.ReadSegment> segs = ringByteBuf.poll();

            if (segs == null)
                return 0;

            int written = 0;

            for (SegmentedRingByteBuffer.ReadSegment seg : segs) {
                updateHeartbeat();

                try {
                    written += fileIo.writeFully(seg.buffer());
                }
                finally {
                    seg.release();
                }
            }

            return written;
        }
    }
}
