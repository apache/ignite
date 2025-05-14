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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FLUSH_SIZE;
import static org.apache.ignite.internal.IgniteKernal.CFG_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_METRICS_SYS_VIEW;
import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_IO_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.PART_STATES_VIEW;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.BINARY_METADATA_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.METASTORE_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.DATA_REGION_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.PAGE_TS_HISTOGRAM_VIEW;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODES_SYS_VIEW;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LATCHES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LOCKS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LONGS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.QUEUES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.REFERENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEMAPHORES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEQUENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SETS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.STAMPED_VIEW;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl.DISTRIBUTED_METASTORE_VIEW;
import static org.apache.ignite.internal.processors.metric.SqlViewMetricExporterSpi.SYS_VIEW_NAME;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_ATTR_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.DFLT_BUFFER_SIZE;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.DFLT_FLUSH_SIZE;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.FILE_FORMAT_VERSION;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.resolveStatisticsFile;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.writeString;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_ROW;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_SCHEMA;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_PLAN_HIST_VIEW;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_HIST_VIEW;
import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_VIEW;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_IDXS_VIEW;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_SCHEMA_VIEW;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_TBLS_VIEW;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_TBL_COLS_VIEW;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_VIEWS_VIEW;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_VIEW_COLS_VIEW;
import static org.apache.ignite.internal.processors.query.stat.IgniteGlobalStatisticsManager.STAT_GLOBAL_VIEW_NAME;
import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsConfigurationManager.STAT_CFG_VIEW_NAME;
import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository.STAT_LOCAL_DATA_VIEW;
import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository.STAT_PART_DATA_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.spi.systemview.view.SnapshotView.SNAPSHOT_SYS_VIEW;

/** Worker to write system views to performance statistics file. */
class SystemViewFileWriter extends GridWorker {
    /** Default system views. */
    private static final List<String> SYSTEM_VIEWS = new ArrayList<>(List.of(
        CACHES_VIEW,
        CACHE_GRPS_VIEW,
        TASKS_VIEW,
        JOBS_VIEW,
        SVCS_VIEW,
        TXS_MON_LIST,
        NODES_SYS_VIEW,
        NODE_ATTRIBUTES_SYS_VIEW,
        CFG_VIEW,
        BASELINE_NODES_SYS_VIEW,
        BASELINE_NODE_ATTRIBUTES_SYS_VIEW,
        CLI_CONN_VIEW,
        CLI_CONN_ATTR_VIEW,
        SYS_POOL_QUEUE_VIEW,
        STREAM_POOL_QUEUE_VIEW,
        SCAN_QRY_SYS_VIEW,
        CQ_SYS_VIEW,
        SQL_QRY_VIEW,
        SQL_QRY_HIST_VIEW,
        SQL_SCHEMA_VIEW,
        NODE_METRICS_SYS_VIEW,
        SQL_TBLS_VIEW,
        SQL_TBL_COLS_VIEW,
        SQL_VIEWS_VIEW,
        SQL_VIEW_COLS_VIEW,
        SQL_IDXS_VIEW,
        CACHE_GRP_PAGE_LIST_VIEW,
        DATA_REGION_PAGE_LIST_VIEW,
        PART_STATES_VIEW,
        BINARY_METADATA_VIEW,
        METASTORE_VIEW,
        DISTRIBUTED_METASTORE_VIEW,
        QUEUES_VIEW,
        SETS_VIEW,
        SEQUENCES_VIEW,
        LONGS_VIEW,
        REFERENCES_VIEW,
        STAMPED_VIEW,
        LATCHES_VIEW,
        SEMAPHORES_VIEW,
        LOCKS_VIEW,
        STAT_CFG_VIEW_NAME,
        STAT_LOCAL_DATA_VIEW,
        STAT_PART_DATA_VIEW,
        STAT_GLOBAL_VIEW_NAME,
        SNAPSHOT_SYS_VIEW,
        CACHE_GRP_IO_VIEW,
        SQL_PLAN_HIST_VIEW,
        PAGE_TS_HISTOGRAM_VIEW,
        SQL_TBL_COLS_VIEW,
        SYS_VIEW_NAME
    ));

    /** File writer thread name. */
    private static final String SYSTEM_VIEW_WRITER_THREAD_NAME = "performance-statistics-system-view-writer";

    /** Performance statistics system view file. */
    private final String filePath;

    /** Performance statistics file I/O. */
    private final FileIO fileIo;

    /** */
    private final int flushSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_FLUSH_SIZE, DFLT_FLUSH_SIZE);

    /** */
    private final GridSystemViewManager sysViewMgr;

    /** System view predicate to filter recorded views. */
    private final Predicate<SystemView<?>> sysViewPredicate;

    /** Writes system view attributes to {@link SystemViewFileWriter#buf}. */
    private final SystemViewRowAttributeWalker.AttributeWithValueVisitor valWriterVisitor = new AttributeWithValueWriterVisitor();

    /** */
    private StringCache strCache = new StringCache();

    /** Buffer. */
    private ByteBuffer buf;

    /**
     * @param ctx Kernal context.
     */
    public SystemViewFileWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
        super(ctx.igniteInstanceName(), SYSTEM_VIEW_WRITER_THREAD_NAME, ctx.log(SystemViewFileWriter.class));

        sysViewMgr = ctx.systemView();

        File file = resolveStatisticsFile(ctx, "node-" + ctx.localNodeId() + "-system-views");

        fileIo = new RandomAccessFileIOFactory().create(file);

        filePath = file.getPath();

        int bufSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_BUFFER_SIZE, DFLT_BUFFER_SIZE);
        buf = ByteBuffer.allocateDirect(bufSize);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        // System views that won't be recorded. They may be large or copy another PerfStat values.
        Set<String> ignoredViews = Set.of("baseline.node.attributes",
            "node.attributes",
            "metrics",
            "caches",
            "sql.queries",
            "nodes");
        sysViewPredicate = view -> !ignoredViews.contains(view.name());

        doWrite(buf -> {
            buf.put(OperationType.VERSION.id());
            buf.putShort(FILE_FORMAT_VERSION);
        });
    }

    /** {@inheritDoc} */
    @Override protected void body() {
        if (log.isInfoEnabled())
            log.info("Started writing system views to " + filePath + ".");

        try {
            for (String viewName : SYSTEM_VIEWS) {
                SystemView<?> view = sysViewMgr.view(viewName);
                if (view == null)
                    continue;

                try {
                    if (sysViewPredicate.test(view))
                        systemView(view);
                }
                catch (BufferOverflowException e) {
                    throw e;
                }
                catch (AssertionError /* TODO: IGNITE-25152 */ | RuntimeException e) {
                    log.warning("Unable to write system view: " + view.name() + ".", e);
                }
            }

            flush();

            if (log.isInfoEnabled())
                log.info("Finished writing system views to performance statistics file: " + filePath + '.');
        }
        catch (IOException | BufferOverflowException e) {
            log.error("Unable to write to the performance statistics file: " + filePath + '.', e);
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

    /** */
    private void systemView(SystemView<?> view) throws IOException {
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

    /** */
    private void flush() throws IOException {
        buf.flip();
        fileIo.writeFully(buf);
        buf.clear();
    }

    /** Writes view row to file. */
    private class AttributeWithValueWriterVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {
        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
            String str = String.valueOf(val);
            writeString(buf, str, strCache.cacheIfPossible(str));
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
