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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FILE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FLUSH_SIZE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CHECKPOINT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.PAGES_WRITE_THROTTLE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheStartRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.checkpointRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.pagesWriteThrottleRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryReadsRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.taskRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.transactionRecordSize;

/**
 * Performance statistics writer based on logging to a file.
 * <p>
 * Each node collects statistics to a file placed under {@link #PERF_STAT_DIR}.
 * <p>
 * To iterate over records use {@link FilePerformanceStatisticsReader}.
 */
public class FilePerformanceStatisticsWriter {
    /** Directory to store performance statistics files. Placed under Ignite work directory. */
    public static final String PERF_STAT_DIR = "perf_stat";

    /** Default maximum file size in bytes. Performance statistics will be stopped when the size exceeded. */
    public static final long DFLT_FILE_MAX_SIZE = 32 * U.GB;

    /** Default off heap buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = (int)(32 * U.MB);

    /** Default minimal batch size to flush in bytes. */
    public static final int DFLT_FLUSH_SIZE = (int)(8 * U.MB);

    /** Default maximum cached strings threshold. String caching will stop on threshold excess. */
    public static final int DFLT_CACHED_STRINGS_THRESHOLD = 1024;

    /** File writer thread name. */
    static final String WRITER_THREAD_NAME = "performance-statistics-writer";

    /** Minimal batch size to flush in bytes. */
    private final int flushSize =
        IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_FLUSH_SIZE, DFLT_FLUSH_SIZE);

    /** Maximum cached strings threshold. String caching will stop on threshold excess. */
    private final int cachedStrsThreshold =
        IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, DFLT_CACHED_STRINGS_THRESHOLD);

    /** Factory to provide I/O interface. */
    private final FileIOFactory fileIoFactory = new RandomAccessFileIOFactory();

    /** Performance statistics file. */
    private final File file;

    /** Performance statistics file I/O. */
    private final FileIO fileIo;

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

    /** Hashcodes of cached strings. */
    private final Set<Integer> knownStrs = new GridConcurrentHashSet<>();

    /** Count of cached strings. */
    private volatile int knownStrsSz;

    /** @param ctx Kernal context. */
    public FilePerformanceStatisticsWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
        log = ctx.log(getClass());

        file = resolveStatisticsFile(ctx);

        fileIo = fileIoFactory.create(file);

        log.info("Performance statistics file created [file=" + file.getAbsolutePath() + ']');

        long fileMaxSize = IgniteSystemProperties.getLong(IGNITE_PERF_STAT_FILE_MAX_SIZE, DFLT_FILE_MAX_SIZE);
        int bufSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_BUFFER_SIZE, DFLT_BUFFER_SIZE);

        ringByteBuf = new SegmentedRingByteBuffer(bufSize, fileMaxSize, SegmentedRingByteBuffer.BufferMode.DIRECT);

        fileWriter = new FileWriter(ctx, log);
    }

    /** Starts collecting performance statistics. */
    public synchronized void start() {
        assert !started;

        new IgniteThread(fileWriter).start();

        started = true;
    }

    /** Stops collecting performance statistics. */
    public synchronized void stop() {
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

        U.closeQuiet(fileIo);

        knownStrs.clear();

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

    /** @return Performance statistics file. */
    private static File resolveStatisticsFile(GridKernalContext ctx) throws IgniteCheckedException {
        String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

        File fileDir = U.resolveWorkDirectory(igniteWorkDir, PERF_STAT_DIR, false);

        File file = new File(fileDir, "node-" + ctx.localNodeId() + ".prf");;

        int idx = 0;

        while (file.exists()) {
            idx++;

            file = new File(fileDir, "node-" + ctx.localNodeId() + '-' + idx + ".prf");
        }

        return file;
    }

    /** Writes {@link UUID} to buffer. */
    private static void writeUuid(ByteBuffer buf, UUID uuid) {
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
    }

    /** Writes {@link IgniteUuid} to buffer. */
    static void writeIgniteUuid(ByteBuffer buf, IgniteUuid uuid) {
        buf.putLong(uuid.globalId().getMostSignificantBits());
        buf.putLong(uuid.globalId().getLeastSignificantBits());
        buf.putLong(uuid.localId());
    }

    /**
     * @param buf Buffer to write to.
     * @param str String to write.
     * @param cached {@code True} if string cached.
     */
    static void writeString(ByteBuffer buf, String str, boolean cached) {
        buf.put(cached ? (byte)1 : 0);

        if (cached)
            buf.putInt(str.hashCode());
        else {
            byte[] bytes = str.getBytes();

            buf.putInt(bytes.length);
            buf.put(bytes);
        }
    }

    /** @return {@code True} if string was cached and can be written as hashcode. */
    private boolean cacheIfPossible(String str) {
        if (knownStrsSz >= cachedStrsThreshold)
            return false;

        int hash = str.hashCode();

        // We can cache slightly more strings then threshold value.
        // Don't implement solution with synchronization here, because our primary goal is avoid any contention.
        if (knownStrs.contains(hash) || !knownStrs.add(hash))
            return true;

        knownStrsSz = knownStrs.size();

        return false;
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
