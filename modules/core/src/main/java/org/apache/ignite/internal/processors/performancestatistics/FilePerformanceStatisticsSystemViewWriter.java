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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;

/**
 * Performance statistics writer based on logging to a file.
 * <p>
 * Each node collects statistics to a file placed under {@link #PERF_STAT_DIR}.
 * <p>
 * To iterate over records use {@link FilePerformanceStatisticsReader}.
 */
public class FilePerformanceStatisticsSystemViewWriter extends GridWorker {
    /**
     * Directory to store performance statistics files. Placed under Ignite work directory.
     */
    public static final String PERF_STAT_DIR = "perf_stat";

    /**
     * Default maximum file size in bytes. Performance statistics will be stopped when the size exceeded.
     */
    public static final long DFLT_FILE_MAX_SIZE = 32 * U.GB;

    /**
     * Default maximum cached strings threshold. String caching will stop on threshold excess.
     */
    public static final int DFLT_CACHED_STRINGS_THRESHOLD = 10 * 1024;

    /**
     * File format version. This version should be incremented each time when format of existing events are
     * changed (fields added/removed) to avoid unexpected non-informative errors on deserialization.
     */
    public static final short FILE_FORMAT_VERSION = 1;

    /**
     * File writer thread name.
     */
    private static final String WRITER_THREAD_NAME = "performance-statistics-system-view-writer";

    /**
     * Maximum cached strings threshold. String caching will stop on threshold excess.
     */
    private final int cachedStrsThreshold =
        IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, DFLT_CACHED_STRINGS_THRESHOLD);

    /** */
    private final RandomAccessFile file;

    /** System view manager. */
    private final GridSystemViewManager sysViewMngr;

    /** Logger.*/
    private final IgniteLogger log;
    /**
     * File writer thread started flag.
     */
    private boolean started;

    /**
     * @param ctx Kernal context.
     */
    public FilePerformanceStatisticsSystemViewWriter(GridKernalContext ctx) throws IgniteCheckedException, IOException {
        log = ctx.log(getClass());

        sysViewMngr = ctx.systemView();

        file = resolveStatisticsFile(ctx);
    }

    /**
     * @return Performance statistics file.
     */
    private RandomAccessFile resolveStatisticsFile(
        GridKernalContext ctx) throws IgniteCheckedException, FileNotFoundException {
        String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

        File fileDir = U.resolveWorkDirectory(igniteWorkDir, PERF_STAT_DIR, false);

        File file = new File(fileDir, "node-" + ctx.localNodeId() + "-system-view.prf");
        ;

        int idx = 0;

        while (file.exists()) {
            idx++;

            file = new File(fileDir, "node-" + ctx.localNodeId() + '-' + idx + ".prf");
        }
        log.info("Performance statistics system view file created [file=" + file.getAbsolutePath() + ']');

        return new RandomAccessFile(file.getAbsolutePath(), "rw");
    }

    /**
     * Hashcodes of cached strings.
     */
    private final Set<Integer> knownStrs = new HashSet<>();

    /**
     * @param ctx Kernal context.
     * @param log Logger.
     */
    FileWriter(GridKernalContext ctx, IgniteLogger log) {
        super(ctx.igniteInstanceName(), WRITER_THREAD_NAME, log, ctx.workersRegistry());
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        try {
            file.writeShort(FILE_FORMAT_VERSION);
            file.write(OperationType.VERSION.id());
        }
        catch (IOException e) {
            log.error("Failed to write system view statistics file", e);
        }

        sysViewMngr.forEach(view -> {
            try {
                systemView(view);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * @param view System view to write.
     */
    public void systemView(SystemView<?> view) throws IOException {
        // TODO: Write view.name()
        long startPos = file.getFilePointer();

        file.skipBytes(Long.BYTES);

        SystemViewRowAttributeWalker<Object> walker = ((SystemView<Object>)view).walker();

        AttributeWriterVisitor attrVisitor = new AttributeWriterVisitor();
        walker.visitAll(attrVisitor);

        AttributeWithValueWriterVisitor valVisitor = new AttributeWithValueWriterVisitor();
        view.forEach(row -> walker.visitAll(row, valVisitor));

        long endPos = file.getFilePointer();

        long recSize = endPos - startPos;

        file.seek(startPos);

        file.writeLong(recSize);

        file.seek(endPos);
    }

    /**
     * @return {@code True} if string was cached and can be written as hashcode.
     */
    private boolean cacheIfPossible(String str) {
        if (knownStrs.size() >= cachedStrsThreshold)
            return false;

        int hash = str.hashCode();

        return knownStrs.contains(hash) || !knownStrs.add(hash);
    }

    /** Write schema of system view to file. */
    private class AttributeWriterVisitor implements SystemViewRowAttributeWalker.AttributeVisitor {
        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz) {
            try {
                writeString(name);

                if (clazz.isPrimitive())
                    writeString(clazz.getSimpleName());
                else
                    writeString(String.class.getSimpleName());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * @param str String to write.
         */
        private void writeString(String str) throws IOException {
            file.writeByte(cached ? (byte)1 : 0);

            if (cached)
                dataOutputStream.writeInt(str.hashCode());
            else {
                byte[] bytes = str.getBytes();

                dataOutputStream.writeInt(bytes.length);
                dataOutputStream.write(bytes);
            }
        }
    }

    /** Writes view row to file. */
    private class AttributeWithValueWriterVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {

        /**
         *
         */
        private final RandomAccessFile file;

        public AttributeWithValueWriterVisitor(RandomAccessFile file) {
            this.file = file;
        }

        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
            try {
                String str = String.valueOf(val);
                file.writeBytes(str);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptBoolean(int idx, String name, boolean val) {
            try {
                file.writeBoolean(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptChar(int idx, String name, char val) {
            try {
                file.writeChar(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptByte(int idx, String name, byte val) {
            try {
                file.writeByte(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptShort(int idx, String name, short val) {
            try {
                file.writeShort(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptInt(int idx, String name, int val) {
            try {
                file.writeInt(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptLong(int idx, String name, long val) {
            try {
                file.writeLong(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptFloat(int idx, String name, float val) {
            try {
                file.writeFloat(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptDouble(int idx, String name, double val) {
            try {
                file.writeDouble(val);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
}
