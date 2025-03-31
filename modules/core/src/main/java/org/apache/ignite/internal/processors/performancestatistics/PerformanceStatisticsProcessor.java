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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EventListener;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.IndexQueryDesc;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage.IGNITE_INTERNAL_KEY_PREFIX;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.PERFORMANCE_STATISTICS_ROTATE;

/**
 * Performance statistics processor.
 * <p>
 * Manages collecting performance statistics.
 *
 * @see FilePerformanceStatisticsWriter
 * @see FilePerformanceStatisticsReader
 */
public class PerformanceStatisticsProcessor extends GridProcessorAdapter {
    /** Prefix for performance statistics enabled key. */
    private static final String PERF_STAT_KEY = IGNITE_INTERNAL_KEY_PREFIX + "performanceStatistics.enabled";

    /** Performance statistics writer. {@code Null} if collecting statistics disabled. */
    @Nullable private volatile FilePerformanceStatisticsWriter writer;

    /** System view writer. {@code Null} if collecting statistics disabled. */
    @Nullable private AbstractFilePerformanceStatisticsWriter sysViewWriter;

    /** Metastorage with the write access. */
    @Nullable private volatile DistributedMetaStorage metastorage;

    /** Synchronization mutex for start/stop collecting performance statistics operations. */
    private final Object mux = new Object();

    /** Performance statistics state listeners. */
    private final ArrayList<PerformanceStatisticsStateListener> lsnrs = new ArrayList<>();

    /** Rotate performance statistics process. */
    private DistributedProcess<Serializable, Serializable> rotateProc;

    /** @param ctx Kernal context. */
    public PerformanceStatisticsProcessor(GridKernalContext ctx) {
        super(ctx);

        registerStateListener(() -> {
            if (U.isLocalNodeCoordinator(ctx.discovery()))
                ctx.cache().cacheDescriptors().values().forEach(desc -> cacheStart(desc.cacheId(), desc.cacheName()));
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(
            new DistributedMetastorageLifecycleListener() {
                @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                    metastorage.listen(PERF_STAT_KEY::equals, (key, oldVal, newVal) -> {
                        // Skip history on local join.
                        if (!ctx.discovery().localJoinFuture().isDone())
                            return;

                        onMetastorageUpdate((boolean)newVal);
                    });
                }

                @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                    PerformanceStatisticsProcessor.this.metastorage = metastorage;

                    try {
                        Boolean performanceStatsEnabled = metastorage.read(PERF_STAT_KEY);

                        if (performanceStatsEnabled == null)
                            return;

                        onMetastorageUpdate(performanceStatsEnabled);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            });

        rotateProc = new DistributedProcess<>(ctx, PERFORMANCE_STATISTICS_ROTATE,
            req -> ctx.closure().callLocalSafe(() -> {
                rotateWriter();

                return null;
            }),
            (id, res, err) -> {});
    }

    /** Registers state listener. */
    public void registerStateListener(PerformanceStatisticsStateListener lsnr) {
        lsnrs.add(lsnr);
    }

    /**
     * @param cacheId Cache id.
     * @param name Cache name.
     */
    public void cacheStart(int cacheId, String name) {
        write(writer -> writer.cacheStart(cacheId, name));
    }

    /**
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     */
    public void cacheOperation(OperationType type, int cacheId, long startTime, long duration) {
        write(writer -> writer.cacheOperation(type, cacheId, startTime, duration));
    }

    /**
     * @param cacheIds Cache IDs.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param commited {@code True} if commited.
     */
    public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commited) {
        write(writer -> writer.transaction(cacheIds, startTime, duration, commited));
    }

    /**
     * @param type Cache query type.
     * @param text Query text in case of SQL query. Cache name in case of SCAN query.
     *             In case of an INDEX query, the text represents the pattern:
     *             <pre>{@code <cacheName>:<indexName>:<valueType>:<comma separated fields>}</pre>
     * @param id Query id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param success Success flag.
     */
    public void query(GridCacheQueryType type, String text, long id, long startTime, long duration, boolean success) {
        write(writer -> writer.query(type, text, id, startTime, duration, success));
    }

    /**
     * @param type Cache query type.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param logicalReads Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads, long physicalReads) {
        write(writer -> writer.queryReads(type, queryNodeId, id, logicalReads, physicalReads));
    }

    /**
     * @param type Cache query type.
     * @param qryNodeId Originating node id.
     * @param id Query id.
     * @param action Action with rows.
     * @param rows Number of rows processed.
     */
    public void queryRowsProcessed(GridCacheQueryType type, UUID qryNodeId, long id, String action, long rows) {
        write(writer -> writer.queryRows(type, qryNodeId, id, action, rows));
    }

    /**
     * @param type Cache query type.
     * @param qryNodeId Originating node id.
     * @param id Query id.
     * @param name Query property name.
     * @param val Query property value.
     */
    public void queryProperty(GridCacheQueryType type, UUID qryNodeId, long id, String name, String val) {
        write(writer -> writer.queryProperty(type, qryNodeId, id, name, val));
    }

    /**
     * @param sesId Session id.
     * @param taskName Task name.
     * @param startTime Start time in milliseconds.
     * @param duration Duration.
     * @param affPartId Affinity partition id.
     */
    public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        write(writer -> writer.task(sesId, taskName, startTime, duration, affPartId));
    }

    /**
     * @param sesId Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime Start time in milliseconds.
     * @param duration Job execution time.
     * @param timedOut {@code True} if job is timed out.
     */
    public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        write(writer -> writer.job(sesId, queuedTime, startTime, duration, timedOut));
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
        long recoveryDataWriteDuration,
        long totalDuration,
        long cpStartTime,
        int pagesSize,
        int dataPagesWritten,
        int cowPagesWritten
    ) {
        write(writer -> writer.checkpoint(beforeLockDuration,
            lockWaitDuration,
            listenersExecDuration,
            markDuration,
            lockHoldDuration,
            pagesWriteDuration, fsyncDuration,
            walCpRecordFsyncDuration,
            writeCpEntryDuration,
            splitAndSortCpPagesDuration,
            recoveryDataWriteDuration,
            totalDuration,
            cpStartTime,
            pagesSize,
            dataPagesWritten,
            cowPagesWritten));
    }

    /**
     * @param endTime End time in milliseconds.
     * @param duration Duration in milliseconds.
     */
    public void pagesWriteThrottle(long endTime, long duration) {
        write(writer -> writer.pagesWriteThrottle(endTime, duration));
    }

    /**
     * Starts collecting performance statistics.
     *
     * @throws IgniteCheckedException If starting failed.
     */
    public void startCollectStatistics() throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (ctx.isStopping())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

        metastorage.write(PERF_STAT_KEY, true);
    }

    /**
     * Stops collecting performance statistics.
     *
     * @throws IgniteCheckedException If stopping failed.
     */
    public void stopCollectStatistics() throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (ctx.isStopping())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

        metastorage.write(PERF_STAT_KEY, false);
    }

    /**
     * Rotate collecting performance statistics.
     *
     * @throws IgniteCheckedException If rotation failed.
     */
    public void rotateCollectStatistics() throws IgniteCheckedException {
        if (ctx.isStopping())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

        if (!enabled())
            throw new IgniteCheckedException("Performance statistics collection not started.");

        rotateProc.start(UUID.randomUUID(), null);
    }

    /** @return {@code True} if collecting performance statistics is enabled. */
    public boolean enabled() {
        return writer != null;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (enabled())
            stopWriter();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        if (enabled())
            stopWriter();
    }

    /** Starts or stops collecting statistics on metastorage update. */
    private void onMetastorageUpdate(boolean start) {
        ctx.closure().runLocalSafe((GridPlainRunnable)() -> {
            if (start)
                startWriter();
            else
                stopWriter();
        });
    }

    /** Starts performance statistics writer. */
    private void startWriter() {
        try {
            synchronized (mux) {
                if (writer != null)
                    return;

                writer = new FilePerformanceStatisticsWriter(ctx);
                sysViewWriter = new FilePerformanceStatisticsSystemViewWriter(ctx);

                writer.start();
                sysViewWriter.start();
            }

            lsnrs.forEach(PerformanceStatisticsStateListener::onStarted);

            log.info("Performance statistics writer started.");
        }
        catch (Exception e) {
            log.error("Failed to start performance statistics writer.", e);
        }
    }

    /** Stops performance statistics writer. */
    private void stopWriter() {
        synchronized (mux) {
            if (writer == null)
                return;

            FilePerformanceStatisticsWriter writer = this.writer;
            AbstractFilePerformanceStatisticsWriter sysViewWriter = this.sysViewWriter;

            this.writer = null;
            this.sysViewWriter = null;

            writer.stop();
            sysViewWriter.stop();
        }

        log.info("Performance statistics writer stopped.");
    }

    /** Rotate performance statistics writer. */
    private void rotateWriter() throws Exception {
        FilePerformanceStatisticsWriter oldWriter = null;

        synchronized (mux) {
            if (writer == null)
                return;

            FilePerformanceStatisticsWriter newWriter = new FilePerformanceStatisticsWriter(ctx);

            newWriter.start();

            oldWriter = writer;

            writer = newWriter;

            oldWriter.stop();
        }

        if (log.isInfoEnabled() && oldWriter != null)
            log.info("Performance statistics writer rotated[writtenFile=" + oldWriter.file() + "].");
    }

    /** Writes statistics through passed writer. */
    private void write(Consumer<FilePerformanceStatisticsWriter> c) {
        FilePerformanceStatisticsWriter writer = this.writer;

        if (writer != null)
            c.accept(writer);
    }

    /** Performance statistics state listener. */
    public interface PerformanceStatisticsStateListener extends EventListener {
        /** This method is called whenever the performance statistics collecting is started. */
        public void onStarted();
    }

    /** @return Text representation of index query. */
    public static String indexQueryText(String cacheName, IndexQueryDesc desc) {
        StringBuilder s = new StringBuilder();

        s.append(cacheName);
        s.append(':');
        s.append(desc.idxName());
        s.append(':');
        s.append(desc.valType());
        s.append(':');
        s.append(String.join(",", F.viewReadOnly(desc.criteria(), IndexQueryCriterion::field)));

        return s.toString();
    }
}
