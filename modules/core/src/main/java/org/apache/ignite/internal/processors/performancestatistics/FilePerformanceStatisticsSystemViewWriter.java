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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_ROW;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_SCHEMA;

/**
 * Performance statistics writer to record system views.
 * <p>
 * Each node collects statistics to a file placed under {@link #PERF_STAT_DIR}.
 * <p>
 * To iterate over records use {@link FilePerformanceStatisticsReader}.
 */
public class FilePerformanceStatisticsSystemViewWriter extends AbstractFilePerformanceStatisticsWriter {
    /** File writer thread name. */
    static final String WRITER_THREAD_NAME = "performance-statistics-system-view-writer";

    /** Logger. */
    private final IgniteLogger log;

    /** Buffer. */
    private final ByteBuffer buf;

    /** Writer. */
    private final FileWriter writer;

    /**  */
    private final GridSystemViewManager sysViewMgr;

    /** System view predicate to filter recorded views. */
    private final Predicate<SystemView<?>> sysViewPredicate;

    /**
     * @param ctx Kernal context.
     */
    public FilePerformanceStatisticsSystemViewWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
        super(ctx, "node-" + ctx.localNodeId() + "-system-views");

        log = ctx.log(getClass());
        sysViewMgr = ctx.systemView();
        writer = new FileWriter(ctx, log);

        buf = ByteBuffer.allocateDirect(bufSize);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        // System views that won't be recorded. They may be large or copy another PerfStat values.
        Set<String> ignoredViews = Set.of("baseline.node.attributes",
            "metrics",
            "caches",
            "sql.queries",
            "nodes");
        sysViewPredicate = view -> !ignoredViews.contains(view.name());
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (file == null)
            return;

        new IgniteThread(writer).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        U.awaitForWorkersStop(Collections.singleton(writer), true, log);
    }

    /** Worker to write to performance statistics file. */
    private class FileWriter extends GridWorker {
        /**
         * @param ctx Kernal context.
         * @param log Logger.
         */
        FileWriter(GridKernalContext ctx, IgniteLogger log) {
            super(ctx.igniteInstanceName(), WRITER_THREAD_NAME, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            try {
                buf.put(OperationType.VERSION.id());
                buf.putShort(FILE_FORMAT_VERSION);

                for (SystemView<?> view : sysViewMgr) {
                    if (isCancelled())
                        break;
                    if (sysViewPredicate.test(view))
                        systemView(view);
                }

                flush();
                fileIo.force();

                log.info("Finished writing system views to performance statistics file: " + file + '.');
            }
            catch (IOException e) {
                log.error("Unable to write to the performance statistics file.", e);
            }
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            FilePerformanceStatisticsSystemViewWriter.this.cleanup();
        }

        /**  */
        public void systemView(SystemView<?> view) throws IOException {
            SystemViewRowAttributeWalker<Object> walker = ((SystemView<Object>)view).walker();

            AttributeWithValueWriterVisitor valVisitor = new AttributeWithValueWriterVisitor();

            writeSchemaToBuf(walker, view.name());

            for (Object row : view)
                writeRowToBuf(row, valVisitor, walker);
        }

        /**
         * @param walker Walker to visit view attributes.
         * @param viewName View name.
         */
        private void writeSchemaToBuf(SystemViewRowAttributeWalker<Object> walker, String viewName) throws IOException {
            doWrite(buffer -> {
                buffer.put(SYSTEM_VIEW_SCHEMA.id());
                writeString(buf, viewName, cacheIfPossible(viewName));
                writeString(buf, walker.getClass().getName(), cacheIfPossible(walker.getClass().getName()));
            });
        }

        /**
         * @param row        Row.
         * @param valVisitor Value visitor.
         * @param walker     Walker.
         */
        private void writeRowToBuf(Object row, AttributeWithValueWriterVisitor valVisitor,
            SystemViewRowAttributeWalker<Object> walker) throws IOException {
            if (isCancelled())
                return;

            doWrite(buffer -> {
                buf.put(SYSTEM_VIEW_ROW.id());
                walker.visitAll(row, valVisitor);
            });
        }

        /** */
        private void doWrite(Consumer<ByteBuffer> consumer) throws IOException {
            if (isCancelled())
                return;

            int beginPos = buf.position();
            try {
                consumer.accept(buf);
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
            buf.limit(buf.capacity());
            buf.flip();
            buf.clear();
        }
    }

    /** {@inheritDoc} */
    @Override String fileAbsolutePath() {
        return file.getAbsolutePath();
    }

    /** Writes view row to file. */
    private class AttributeWithValueWriterVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {
        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
            writeString(buf, String.valueOf(val), cacheIfPossible(String.valueOf(val)));
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
