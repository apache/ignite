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

package org.apache.ignite.internal;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.profiling.IgniteProfiling;
import org.apache.ignite.internal.profiling.util.ProfilingDeserializer;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static org.apache.ignite.internal.profiling.LogFileProfiling.DFLT_BUFFER_SIZE;
import static org.apache.ignite.internal.profiling.LogFileProfiling.DFLT_FILE_MAX_SIZE;
import static org.apache.ignite.internal.profiling.LogFileProfiling.profilingFile;

/**
 * Test profiling file reader.
 */
public class TestProfilingLogReader implements IgniteProfiling {
    /** IO factory. */
    private static final RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** File read buffer size. */
    private static final int READ_BUFFER_SIZE = 512 * 1024;

    /** File read buffer. */
    private final ByteBuffer readBuf = allocateDirect(READ_BUFFER_SIZE).order(nativeOrder());

    /** Log to write operations to. */
    private final IgniteEx ignite;

    /** Log to write operations to. */
    private final IgniteLogger log;

    /**
     * @param ignite Ignite instance.
     * @param log Log to write operations to.
     */
    TestProfilingLogReader(IgniteEx ignite, IgniteLogger log) {
        this.ignite = ignite;
        this.log = log;
    }

    /** Starts read profiling file to the configured log until node stoped. */
    public void startRead() throws IgniteCheckedException {
        ignite.context().metric().startProfiling(DFLT_FILE_MAX_SIZE, DFLT_BUFFER_SIZE, 0);

        GridTestUtils.runAsync(() -> {
            try (
                FileIO io = ioFactory.create(profilingFile(ignite.context()));
                ProfilingDeserializer des = new ProfilingDeserializer(this)
            ) {
                while (!ignite.context().isStopping()) {
                    int read = io.read(readBuf);

                    readBuf.flip();

                    if (read < 0) {
                        U.sleep(100);

                        continue;
                    }

                    for (;;) {
                        boolean deserialized = des.deserialize(readBuf);

                        if (!deserialized)
                            break;
                    }

                    readBuf.compact();
                }
            }

            return null;
        }, "profiling-file-reader-" + ignite.name());
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(CacheOperationType type, int cacheId, long startTime, long duration) {
        log("cacheOperation", "type", type, "cacheId", cacheId, "startTime", startTime,
            "duration", duration);
    }

    /** {@inheritDoc} */
    @Override public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commit) {
        log("transaction", "cacheIds", cacheIds, "startTime", startTime, "duration", duration,
            "commit", commit);
    }

    /** {@inheritDoc} */
    @Override public void query(GridCacheQueryType type, String text, long id, long startTime, long duration,
        boolean success) {
        log("query", "type", type, "text", text, "id", id, "startTime", startTime,
            "duration", duration, "success", success);
    }

    /** {@inheritDoc} */
    @Override public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads) {
        log("queryReads", "type", type, "queryNodeId", queryNodeId, "id", id,
            "logicalReads", logicalReads, "physicalReads", physicalReads);
    }

    /** {@inheritDoc} */
    @Override public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        log("task", "sesId", sesId, "taskName", taskName, "startTime", startTime,
            "duration", duration, "affPartId", affPartId);
    }

    /** {@inheritDoc} */
    @Override public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        log("job", "sesId", sesId, "queuedTime", queuedTime, "startTime", startTime,
            "duration", duration, "timedOut", timedOut);
    }

    /** {@inheritDoc} */
    @Override public void cacheStart(int cacheId, long startTime, String cacheName, @Nullable String groupName,
        boolean userCache) {
        log("cacheStart", "startTime", startTime, "cacheName", cacheName, "groupName", groupName,
            "userCache", userCache);
    }

    /** {@inheritDoc} */
    @Override public void profilingStart(UUID nodeId, String igniteInstanceName, String igniteVersion, long startTime) {
        log("profilingStart", "nodeId", nodeId, "igniteInstanceName", igniteInstanceName,
            "igniteVersion", igniteVersion, "startTime", startTime);
    }

    /**
     * Logs operations.
     *
     * @param category Profile category.
     * @param tuples Tuples to log (key, value).
     */
    private void log(String category, Object... tuples) {
        assert tuples.length % 2 == 0;

        StringBuilder sb = new StringBuilder();

        sb.append(category).append(" [");

        for (int i = 0; i < tuples.length; i += 2) {
            sb.append(tuples[i]).append("=").append(tuples[i + 1]);

            if (i < tuples.length - 2)
                sb.append(", ");
        }

        sb.append(']');

        log.info(sb.toString());
    }
}
