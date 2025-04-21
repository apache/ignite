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
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.DFLT_BUFFER_SIZE;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.DFLT_FLUSH_SIZE;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.FILE_FORMAT_VERSION;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.resolveStatisticsFile;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.writeString;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_ROW;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_SCHEMA;

/** Worker to write system views to performance statistics file. */
class SystemViewFileWriter extends GridWorker {
    /** File writer thread name. */
    private static final String SYSTEM_VIEW_WRITER_THREAD_NAME = "performance-statistics-system-view-writer";

    /** Performance statistics system view file. */
    private final String filePath;

    /** Performance statistics file I/O. */
    private final FileIO fileIo;

    /** */
    private final int flushSize;

    /** */
    private final GridSystemViewManager sysViewMgr;

    /** Writes system view attributes to {@link SystemViewFileWriter#buf}. */
    private final SystemViewRowAttributeWalker.AttributeWithValueVisitor valWriterVisitor;

    /** System view predicate to filter recorded views. */
    private final Predicate<SystemView<?>> sysViewPredicate;

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
        if (log.isInfoEnabled())
            log.info("Started writing system views to " + filePath + ".");

        try {
            for (SystemView<?> view : sysViewMgr)
                if (sysViewPredicate.test(view))
                    systemView(view);

            flush();

            if (log.isInfoEnabled())
                log.info("Finished writing system views to performance statistics file: " + filePath + '.');
        }
        catch (IOException e) {
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
