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
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_ROW;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_SCHEMA;

/**
 * Performance statistics writer to record system views.
 * <p>
 * Each node collects statistics to a file placed under {@link FilePerformanceStatisticsWriter#PERF_STAT_DIR}.
 * <p>
 * To iterate over records use {@link FilePerformanceStatisticsReader}.
 */
public class FilePerformanceStatisticsSystemViewWriter {
    /** Directory to store performance statistics files. Placed under Ignite work directory. */
    public static final String PERF_STAT_DIR = "perf_stat";

    /** Default off heap buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = (int)(32 * U.MB);

    /** Default maximum cached strings threshold. String caching will stop on threshold excess. */
    public static final int DFLT_CACHED_STRINGS_THRESHOLD = 10 * 1024;

    /**
     * File format version. This version should be incremented each time when format of existing events are
     * changed (fields added/removed) to avoid unexpected non-informative errors on deserialization.
     */
    public static final short FILE_FORMAT_VERSION = 1;

    /** File writer thread name. */
    static final String WRITER_THREAD_NAME = "performance-statistics-system-view-writer";

    /** Performance statistics file. */
    protected final File file;

    /** Performance statistics file I/O. */
    protected final FileIO fileIo;

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

    /** Maximum cached strings threshold. String caching will stop on threshold excess. */
    private final int cachedStrsThreshold = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD,
        DFLT_CACHED_STRINGS_THRESHOLD);

    /**  */
    protected int bufSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_BUFFER_SIZE, DFLT_BUFFER_SIZE);

    /** Hashcodes of cached strings. */
    private Set<Integer> knownStrs = new GridConcurrentHashSet<>();

    /** Count of cached strings. */
    private volatile int knownStrsSz;

    /**
     * @param ctx Kernal context.
     */
    public FilePerformanceStatisticsSystemViewWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
        file = resolveStatisticsFile(ctx, "node-" + ctx.localNodeId() + "-system-views");
        fileIo = new RandomAccessFileIOFactory().create(file);

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
            "nodes",
            "partitionStates",
            "statisticsPartitionData");
        sysViewPredicate = view -> !ignoredViews.contains(view.name());
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
     * @param buf    Buffer to write to.
     * @param str    String to write.
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

    /** */
    public void start() {
        new IgniteThread(writer).start();
    }

    /** */
    public void stop() {
        U.awaitForWorkersStop(Collections.singleton(writer), true, log);
    }

    /** @return {@code True} if string was cached and can be written as hashcode. */
    protected boolean cacheIfPossible(String str) {
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

    /** */
    protected void cleanup() {
        U.closeQuiet(fileIo);
        knownStrs = null;
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
            doWrite(buffer -> {
                buf.put(SYSTEM_VIEW_ROW.id());
                walker.visitAll(row, valVisitor);
            });
        }

        /** Write to {@link  FilePerformanceStatisticsSystemViewWriter#buf} and handle overflow if necessary. */
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
            buf.flip();
            buf.clear();
        }
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
