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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FILE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FLUSH_SIZE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CHECKPOINT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.PAGES_WRITE_THROTTLE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_PROPERTY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_ROWS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_ROW;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_SCHEMA;
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
public class FilePerformanceStatisticsWriter {
    /** Directory to store performance statistics files. Placed under Ignite work directory. */
    public static final String PERF_STAT_DIR = "perf_stat";

    /** Default maximum file size in bytes. Performance statistics will be stopped when the size exceeded. */
    public static final long DFLT_FILE_MAX_SIZE = 32 * U.GB;

    /** Default off heap buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = (int)(32 * U.MB);

    /** Default minimal batch size to flush in bytes. */
    public static final int DFLT_FLUSH_SIZE = (int)(8 * U.MB);

    /**
     * File format version. This version should be incremented each time when format of existing events are
     * changed (fields added/removed) to avoid unexpected non-informative errors on deserialization.
     */
    public static final short FILE_FORMAT_VERSION = 1;

    /** Default maximum cached strings threshold. String caching will stop on threshold excess. */
    public static final int DFLT_CACHED_STRINGS_THRESHOLD = 10 * 1024;

    /** Performance statistics file writer worker. */
    private volatile FileWriter fileWriter;

    /** Performance statistics system view file writer worker. */
    private final SystemViewFileWriter sysViewFileWriter;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** @param ctx Kernal context. */
    public FilePerformanceStatisticsWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
        this.ctx = ctx;
        log = ctx.log(getClass());

        fileWriter = new FileWriter(ctx);
        sysViewFileWriter = new SystemViewFileWriter(ctx);
    }

    /** Starts collecting performance statistics. */
    public void start() throws IOException {
        fileWriter.doWrite(OperationType.VERSION, OperationType.versionRecordSize(), buf -> buf.putShort(FILE_FORMAT_VERSION));

        new IgniteThread(fileWriter).start();
        new IgniteThread(sysViewFileWriter).start();
    }

    /** Stops collecting performance statistics. */
    public void stop() {
        U.awaitForWorkersStop(List.of(fileWriter, sysViewFileWriter), true, log);
    }

    /** */
    public void rotate() throws IgniteCheckedException, IOException {
        FileWriter newWriter = new FileWriter(ctx);
        newWriter.doWrite(OperationType.VERSION, OperationType.versionRecordSize(), buf -> buf.putShort(FILE_FORMAT_VERSION));

        new IgniteThread(newWriter).start();

        FileWriter oldWriter = fileWriter;

        fileWriter = newWriter;

        U.awaitForWorkersStop(Collections.singleton(oldWriter), true, log);

        if (log.isInfoEnabled())
            log.info("Performance statistics writer rotated[writtenFile=" + oldWriter.file + "].");
    }

    /**
     * @param cacheId Cache id.
     * @param name Cache name.
     */
    public void cacheStart(int cacheId, String name) {
        boolean cached = fileWriter.cacheIfPossible(name);

        fileWriter.doWrite(CACHE_START, cacheStartRecordSize(cached ? 0 : name.getBytes().length, cached), buf -> {
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
        fileWriter.doWrite(type, cacheRecordSize(), buf -> {
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
        fileWriter.doWrite(commited ? TX_COMMIT : TX_ROLLBACK, transactionRecordSize(cacheIds.size()), buf -> {
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
        boolean cached = fileWriter.cacheIfPossible(text);

        fileWriter.doWrite(QUERY, queryRecordSize(cached ? 0 : text.getBytes().length, cached), buf -> {
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
     * @param qryNodeId Originating node id.
     * @param id Query id.
     * @param logicalReads Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    public void queryReads(GridCacheQueryType type, UUID qryNodeId, long id, long logicalReads, long physicalReads) {
        fileWriter.doWrite(QUERY_READS, queryReadsRecordSize(), buf -> {
            buf.put((byte)type.ordinal());
            writeUuid(buf, qryNodeId);
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
        boolean cached = fileWriter.cacheIfPossible(action);

        fileWriter.doWrite(QUERY_ROWS, queryRowsRecordSize(cached ? 0 : action.getBytes().length, cached), buf -> {
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

        boolean cachedName = fileWriter.cacheIfPossible(name);
        boolean cachedVal = fileWriter.cacheIfPossible(val);

        fileWriter.doWrite(QUERY_PROPERTY,
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
        boolean cached = fileWriter.cacheIfPossible(taskName);

        fileWriter.doWrite(TASK, taskRecordSize(cached ? 0 : taskName.getBytes().length, cached), buf -> {
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
        fileWriter.doWrite(JOB, jobRecordSize(), buf -> {
            writeIgniteUuid(buf, sesId);
            buf.putLong(queuedTime);
            buf.putLong(startTime);
            buf.putLong(duration);
            buf.put(timedOut ? (byte)1 : 0);
        });
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
        fileWriter.doWrite(CHECKPOINT, checkpointRecordSize(), buf -> {
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
        fileWriter.doWrite(PAGES_WRITE_THROTTLE, pagesWriteThrottleRecordSize(), buf -> {
            buf.putLong(endTime);
            buf.putLong(duration);
        });
    }

    /** Writes {@link UUID} to buffer. */
    static void writeUuid(ByteBuffer buf, UUID uuid) {
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

    /** @return Performance statistics file. */
    private static File resolveStatisticsFile(GridKernalContext ctx, String fileName) throws IgniteCheckedException {
        String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

        File fileDir = U.resolveWorkDirectory(igniteWorkDir, PERF_STAT_DIR, false);

        File file = new File(fileDir, fileName + ".prf");

        int idx = 0;

        while (file.exists()) {
            idx++;

            file = new File(fileDir, fileName + '-' + idx + ".prf");
        }

        return file;
    }

    /** Worker to write to performance statistics file. */
    private static class FileWriter extends GridWorker {
        /** File writer thread name. */
        private static final String WRITER_THREAD_NAME = "performance-statistics-writer";

        /** */
        private StringCache strCache = new StringCache();

        /** Performance statistics file I/O. */
        private final FileIO fileIo;

        /** */
        private final File file;

        /** Minimal batch size to flush in bytes. */
        private final int flushSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_FLUSH_SIZE, DFLT_FLUSH_SIZE);

        /** Count of written to buffer bytes. */
        private final AtomicInteger writtenToBuf = new AtomicInteger();

        /** {@code True} if the small buffer warning message logged. */
        private final AtomicBoolean smallBufLogged = new AtomicBoolean();

        /** {@code True} if worker stopped due to maximum file size reached. */
        private final AtomicBoolean stopByMaxSize = new AtomicBoolean();

        /** File write buffer. */
        private final SegmentedRingByteBuffer ringByteBuf;

        /**
         * @param ctx Kernal context.
         */
        public FileWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
            super(ctx.igniteInstanceName(), WRITER_THREAD_NAME, ctx.log(FileWriter.class), ctx.workersRegistry());

            file = resolveStatisticsFile(ctx, "node-" + ctx.localNodeId());

            fileIo = new RandomAccessFileIOFactory().create(file);

            if (log.isInfoEnabled())
                log.info("Performance statistics file created [file=" + file.getAbsolutePath() + ']');

            long fileMaxSize = IgniteSystemProperties.getLong(IGNITE_PERF_STAT_FILE_MAX_SIZE, DFLT_FILE_MAX_SIZE);
            int bufSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_BUFFER_SIZE, DFLT_BUFFER_SIZE);

            ringByteBuf = new SegmentedRingByteBuffer(bufSize, fileMaxSize, SegmentedRingByteBuffer.BufferMode.DIRECT);
        }

        /**
         * @param str String to cache.
         */
        public boolean cacheIfPossible(String str) {
            return strCache.cacheIfPossible(str);
        }

        /** {@inheritDoc} */
        @Override protected void body() {
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

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            // Stop accepting new records.
            ringByteBuf.close();

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

            strCache = null;
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

                if (!isCancelled() && stopByMaxSize.compareAndSet(false, true))
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
                synchronized (this) {
                    notify();
                }
            }
        }
    }

    /** Worker to write to performance statistics file. */
    private static class SystemViewFileWriter extends GridWorker {
        /** File writer thread name. */
        private static final String SYSTEM_VIEW_WRITER_THREAD_NAME = "performance-statistics-system-view-writer";

        /** */
        private StringCache strCache = new StringCache();

        /** Performance statistics system view file. */
        private final File file;

        /** Performance statistics file I/O. */
        private final FileIO fileIo;

        /** Buffer. */
        private ByteBuffer buf;

        /** */
        private final int flushSize;

        /** */
        private final GridSystemViewManager sysViewMgr;

        /** Writes system view attributes to {@link SystemViewFileWriter#buf}. */
        private final SystemViewRowAttributeWalker.AttributeWithValueVisitor valWriterVisitor;

        /** System view predicate to filter recorded views. */
        private final Predicate<SystemView<?>> sysViewPredicate;

        /**
         * @param ctx Kernal context.
         */
        public SystemViewFileWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
            super(ctx.igniteInstanceName(), SYSTEM_VIEW_WRITER_THREAD_NAME, ctx.log(SystemViewFileWriter.class));

            sysViewMgr = ctx.systemView();

            file = resolveStatisticsFile(ctx, "node-" + ctx.localNodeId() + "-system-views");
            fileIo = new RandomAccessFileIOFactory().create(file);

            int bufSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_BUFFER_SIZE, DFLT_BUFFER_SIZE);
            buf = ByteBuffer.allocateDirect(bufSize);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            flushSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_FLUSH_SIZE, DFLT_FLUSH_SIZE);

            valWriterVisitor = new AttributeWithValueWriterVisitor(buf);

            // System views that won't be recorded. They may be large or copy another PerfStat values.
            Set<String> ignoredViews = Set.of("baseline.node.attributes",
                "metrics",
                "caches",
                "sql.queries",
                "partitionStates", // TODO: IGNITE-25151
                "statisticsPartitionData", // TODO: IGNITE-25152
                "nodes");
            sysViewPredicate = view -> !ignoredViews.contains(view.name());

            doWrite(buf -> {
                buf.put(OperationType.VERSION.id());
                buf.putShort(FILE_FORMAT_VERSION);
            });
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            try {
                for (SystemView<?> view : sysViewMgr)
                    if (sysViewPredicate.test(view))
                        systemView(view);

                flush();

                if (log.isInfoEnabled())
                    log.info("Finished writing system views to performance statistics file: " + file + '.');
            }
            catch (IOException e) {
                log.error("Unable to write to the performance statistics file: " + file + '.', e);
            }
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            try {
                fileIo.force();
            }
            catch (IOException e) {
                log.warning("Failed to fsync the performance statistics system view file.", e);
            }

            U.closeQuiet(fileIo);

            strCache = null;

            buf = null;
        }

        /**  */
        public void systemView(SystemView<?> view) throws IOException {
            SystemViewRowAttributeWalker<Object> walker = ((SystemView<Object>)view).walker();

            writeSchemaToBuf(walker, view.name());

            for (Object row : view)
                writeRowToBuf(row, walker);
        }

        /**
         * @param walker Walker to visit view attributes.
         * @param viewName View name.
         */
        private void writeSchemaToBuf(SystemViewRowAttributeWalker<Object> walker, String viewName) throws IOException {
            doWrite(buf -> {
                buf.put(SYSTEM_VIEW_SCHEMA.id());
                writeString(buf, viewName, strCache.cacheIfPossible(viewName));
                writeString(buf, walker.getClass().getName(), strCache.cacheIfPossible(walker.getClass().getName()));
            });
        }

        /**
         * @param row Row.
         * @param walker Walker.
         */
        private void writeRowToBuf(Object row, SystemViewRowAttributeWalker<Object> walker) throws IOException {
            doWrite(buf -> {
                buf.put(SYSTEM_VIEW_ROW.id());
                walker.visitAll(row, valWriterVisitor);
            });
        }

        /** Write to {@link  #buf} and handle overflow if necessary. */
        private void doWrite(Consumer<ByteBuffer> consumer) throws IOException {
            if (isCancelled())
                return;

            int beginPos = buf.position();
            try {
                consumer.accept(buf);

                if (buf.position() > flushSize)
                    flush();
            }
            catch (BufferOverflowException e) {
                buf.position(beginPos);
                flush();
                consumer.accept(buf);
            }
        }

        /**  */
        private void flush() throws IOException {
            buf.flip();
            fileIo.writeFully(buf);
            buf.clear();
        }

        /** Writes view row to file. */
        private class AttributeWithValueWriterVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {
            /** */
            private final ByteBuffer buf;

            /**
             * @param buf Buffer to write.
             */
            private AttributeWithValueWriterVisitor(ByteBuffer buf) {
                this.buf = buf;
            }

            /** {@inheritDoc} */
            @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
                writeString(buf, String.valueOf(val), strCache.cacheIfPossible(String.valueOf(val)));
            }

            /** {@inheritDoc} */
            @Override public void acceptBoolean(int idx, String name, boolean val) {
                buf.put(val ? (byte)1 : 0);
            }

            /** {@inheritDoc} */
            @Override public void acceptChar(int idx, String name, char val) {
                buf.putChar(val);
            }

            /** {@inheritDoc} */
            @Override public void acceptByte(int idx, String name, byte val) {
                buf.put(val);
            }

            /** {@inheritDoc} */
            @Override public void acceptShort(int idx, String name, short val) {
                buf.putShort(val);
            }

            /** {@inheritDoc} */
            @Override public void acceptInt(int idx, String name, int val) {
                buf.putInt(val);
            }

            /** {@inheritDoc} */
            @Override public void acceptLong(int idx, String name, long val) {
                buf.putLong(val);
            }

            /** {@inheritDoc} */
            @Override public void acceptFloat(int idx, String name, float val) {
                buf.putFloat(val);
            }

            /** {@inheritDoc} */
            @Override public void acceptDouble(int idx, String name, double val) {
                buf.putDouble(val);
            }
        }
    }
}
