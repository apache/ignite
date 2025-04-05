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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FILE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FLUSH_SIZE;

/**  */
abstract class AbstractFilePerformanceStatisticsWriter {
    /** Directory to store performance statistics files. Placed under Ignite work directory. */
    public static final String PERF_STAT_DIR = "perf_stat";

    /** Default maximum file size in bytes. Performance statistics will be stopped when the size exceeded. */
    public static final long DFLT_FILE_MAX_SIZE = 32 * U.GB;

    /** Default off heap buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = (int)(32 * U.MB);

    /** Default minimal batch size to flush in bytes. */
    public static final int DFLT_FLUSH_SIZE = (int)(8 * U.MB);

    /** Default maximum cached strings threshold. String caching will stop on threshold excess. */
    public static final int DFLT_CACHED_STRINGS_THRESHOLD = 10 * 1024;

    /**
     * File format version. This version should be incremented each time when format of existing events are
     * changed (fields added/removed) to avoid unexpected non-informative errors on deserialization.
     */
    public static final short FILE_FORMAT_VERSION = 1;

    /** Hashcodes of cached strings. */
    protected final Set<Integer> knownStrs = new GridConcurrentHashSet<>();

    /** Minimal batch size to flush in bytes. */
    protected final int flushSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_FLUSH_SIZE, DFLT_FLUSH_SIZE);

    /** Maximum cached strings threshold. String caching will stop on threshold excess. */
    protected final int cachedStrsThreshold = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD,
        DFLT_CACHED_STRINGS_THRESHOLD);

    /** Performance statistics file. */
    protected final File file;

    /** Performance statistics file I/O. */
    protected final FileIO fileIo;

    /** Count of cached strings. */
    private volatile int knownStrsSz;

    /**  */
    protected long fileMaxSize = IgniteSystemProperties.getLong(IGNITE_PERF_STAT_FILE_MAX_SIZE, DFLT_FILE_MAX_SIZE);

    /**  */
    protected int bufSize = IgniteSystemProperties.getInteger(IGNITE_PERF_STAT_BUFFER_SIZE, DFLT_BUFFER_SIZE);

    /**
     * @param ctx Context.
     * @param fileName File name to write.
     */
    protected AbstractFilePerformanceStatisticsWriter(GridKernalContext ctx, String fileName) throws IgniteCheckedException, IOException {
        file = resolveStatisticsFile(ctx, fileName);
        fileIo = new RandomAccessFileIOFactory().create(file);
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

    protected void cleanup() {
        U.closeQuiet(fileIo);
    }

    /** */
    abstract void start();

    /** */
    abstract void stop();
}
