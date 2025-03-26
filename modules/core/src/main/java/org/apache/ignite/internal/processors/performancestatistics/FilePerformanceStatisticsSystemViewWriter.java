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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

/**
 * Performance statistics writer based on logging to a file.
 * <p>
 * Each node collects statistics to a file placed under {@link #PERF_STAT_DIR}.
 * <p>
 * To iterate over records use {@link FilePerformanceStatisticsReader}.
 */
public class FilePerformanceStatisticsSystemViewWriter extends AbstractFilePerformanceStatisticsWriter {
    /**  */
    private RandomAccessFile file;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Kernal context.
     * @param log Logger.
     */
    public FilePerformanceStatisticsSystemViewWriter(GridKernalContext ctx) {
        try {
            file = new RandomAccessFile(resolveStatisticsFile(ctx).getAbsolutePath(), "rw");
        }
        catch (FileNotFoundException | IgniteCheckedException e) {
            file = null;
        }
        log = ctx.log(getClass());
    }

    @Override public void start() {
        try {
            file.write(OperationType.VERSION.id());
            file.write(FILE_FORMAT_VERSION);
        }
        catch (IOException e) {
            log.error("Failed to close statistics file", e);
        }
    }

    @Override public void stop() {
        try {
            file.close();
        }
        catch (IOException e) {
            log.error("Failed to close statistics file", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void systemView(SystemView<?> view) {
        try {
            writeString(file, view.name(), cacheIfPossible(view.name()));

            SystemViewRowAttributeWalker<Object> walker = ((SystemView<Object>)view).walker();

            file.writeInt(walker.count());

            AttributeWriterVisitor attrVisitor = new AttributeWriterVisitor();
            walker.visitAll(attrVisitor);

            AttributeWithValueWriterVisitor valVisitor = new AttributeWithValueWriterVisitor();

            view.forEach(row -> walker.visitAll(row, valVisitor));

        }
        catch (IOException e) {
            log.error("Failed to close statistics file", e);
        }
    }

    /** Write schema of system view to file. */
    private class AttributeWriterVisitor implements SystemViewRowAttributeWalker.AttributeVisitor {
        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz) {
            try {
                writeString(file, name, cacheIfPossible(name));

                if (clazz.isPrimitive())
                    writeString(file, clazz.getName(), cacheIfPossible(clazz.getName()));
                else
                    writeString(file, String.class.getName(), cacheIfPossible(String.class.getName()));
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }
    }

    /** Writes view row to file. */
    private class AttributeWithValueWriterVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {
        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
            try {
                String str = String.valueOf(val);
                writeString(file, str, cacheIfPossible(str));
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptBoolean(int idx, String name, boolean val) {
            try {
                file.writeBoolean(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptChar(int idx, String name, char val) {
            try {
                file.writeChar(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptByte(int idx, String name, byte val) {
            try {
                file.writeByte(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptShort(int idx, String name, short val) {
            try {
                file.writeShort(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptInt(int idx, String name, int val) {
            try {
                file.writeInt(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptLong(int idx, String name, long val) {
            try {
                file.writeLong(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptFloat(int idx, String name, float val) {
            try {
                file.writeFloat(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }

        /** {@inheritDoc} */
        @Override public void acceptDouble(int idx, String name, double val) {
            try {
                file.writeDouble(val);
            }
            catch (IOException e) {
                log.error("Failed to close statistics file", e);
            }
        }
    }
}
