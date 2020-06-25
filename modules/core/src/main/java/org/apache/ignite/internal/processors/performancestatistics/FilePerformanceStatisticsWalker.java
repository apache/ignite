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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.IgnitePerformanceStatistics.CacheOperationType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteUuid;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatistics.readIgniteUuid;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatistics.readUuid;

/**
 * Walker over the performance statistics file.
 *
 * @see FilePerformanceStatistics
 */
public class FilePerformanceStatisticsWalker {
    /** File read buffer size. */
    private static final int READ_BUFFER_SIZE = 8 * 1024 * 1024;

    /** IO factory. */
    private static final RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /**
     * Walks over performance statistics file.
     *
     * @param file Performance statistics file.
     * @param handlers Handlers to process deserialized operation.
     */
    public static void walkFile(Path file, IgnitePerformanceStatistics... handlers) throws IOException {
        ByteBuffer buf = allocateDirect(READ_BUFFER_SIZE).order(nativeOrder());

        try (
            FileIO io = ioFactory.create(file.toFile());
            PerformanceStatisticsDeserializer des = new PerformanceStatisticsDeserializer(handlers)
        ) {
            while (true) {
                int read = io.read(buf);

                buf.flip();

                if (read <= 0)
                    break;

                while (true) {
                    boolean deserialized = des.deserialize(buf);

                    if (!deserialized)
                        break;
                }

                buf.compact();
            }
        }
        finally {
            GridUnsafe.cleanDirectBuffer(buf);
        }
    }

    /**
     * Performance statistics operations deserializer.
     */
    private static class PerformanceStatisticsDeserializer implements AutoCloseable {
        /** Cached strings by id. */
        private final ConcurrentHashMap<Short, String> stringById = new ConcurrentHashMap<>();

        /** Handlers to process deserialized operation. */
        private final IgnitePerformanceStatistics[] handlers;

        /** @param handlers Handlers to process deserialized operation. */
        public PerformanceStatisticsDeserializer(IgnitePerformanceStatistics... handlers) {
            this.handlers = handlers;
        }

        /**
         * Tries to deserialize performance statistics operation from buffer.
         *
         * @param buf Buffer.
         * @return {@code True} if operation parsed. {@code False} if not enough bytes.
         */
        public boolean deserialize(ByteBuffer buf) {
            int pos = buf.position();

            if (buf.remaining() < 1)
                return false;

            byte opTypeByte = buf.get();

            FilePerformanceStatistics.OperationType opType = FilePerformanceStatistics.OperationType.fromOrdinal(opTypeByte);

            switch (opType) {
                case CACHE_OPERATION: {
                    if (buf.remaining() < 1 + 4 + 8 + 8)
                        break;

                    CacheOperationType cacheOp = CacheOperationType.fromOrdinal(buf.get());
                    int cacheId = buf.getInt();
                    long startTime = buf.getLong();
                    long duration = buf.getLong();

                    for (IgnitePerformanceStatistics handler : handlers)
                        handler.cacheOperation(cacheOp, cacheId, startTime, duration);

                    return true;
                }

                case TRANSACTION: {
                    if (buf.remaining() < 4)
                        break;

                    int cacheIdsSize = buf.getInt();

                    if (buf.remaining() < 4 * cacheIdsSize + 8 + 8 + 1)
                        break;

                    GridIntList cacheIds = new GridIntList(cacheIdsSize);

                    for (int i = 0; i < cacheIdsSize; i++)
                        cacheIds.add(buf.getInt());

                    long startTime = buf.getLong();
                    long duration = buf.getLong();
                    boolean commit = buf.get() != 0;

                    for (IgnitePerformanceStatistics handler : handlers)
                        handler.transaction(cacheIds, startTime, duration, commit);

                    return true;
                }

                case QUERY: {
                    if (buf.remaining() < 1 + 1 + 2 + 8 + 8 + 8 + 1)
                        break;

                    GridCacheQueryType queryType = GridCacheQueryType.fromOrdinal(buf.get());
                    boolean needReadString = buf.get() != 0;
                    short strId = buf.getShort();

                    String str;

                    if (needReadString) {
                        int textLength = buf.getInt();

                        if (buf.remaining() < textLength + 8 + 8 + 8 + 1)
                            break;

                        str = readString(buf, textLength);

                        stringById.putIfAbsent(strId, str);
                    }
                    else
                        str = stringById.get(strId);

                    long id = buf.getLong();
                    long startTime = buf.getLong();
                    long duration = buf.getLong();
                    boolean success = buf.get() != 0;

                    if (str == null)
                        return true;

                    for (IgnitePerformanceStatistics handler : handlers)
                        handler.query(queryType, str, id, startTime, duration, success);

                    return true;
                }

                case QUERY_READS: {
                    if (buf.remaining() < 1 + 16 + 8 + 8 + 8)
                        break;

                    GridCacheQueryType queryType = GridCacheQueryType.fromOrdinal(buf.get());

                    UUID uuid = readUuid(buf);
                    long id = buf.getLong();
                    long logicalReads = buf.getLong();
                    long physicalReads = buf.getLong();

                    for (IgnitePerformanceStatistics handler : handlers)
                        handler.queryReads(queryType, uuid, id, logicalReads, physicalReads);

                    return true;
                }

                case TASK: {
                    if (buf.remaining() < 24 + 1 + 2 + 8 + 8 + 4)
                        break;

                    IgniteUuid sesId = readIgniteUuid(buf);
                    boolean needReadString = buf.get() != 0;
                    short strId = buf.getShort();

                    String taskName;

                    if (needReadString) {
                        int textLength = buf.getInt();

                        if (buf.remaining() < textLength + 8 + 8 + 4)
                            break;

                        taskName = readString(buf, textLength);

                        stringById.putIfAbsent(strId, taskName);
                    }
                    else
                        taskName = stringById.get(strId);

                    long startTime = buf.getLong();
                    long duration = buf.getLong();
                    int affPartId = buf.getInt();

                    if (taskName == null)
                        return true;

                    for (IgnitePerformanceStatistics handler : handlers)
                        handler.task(sesId, taskName, startTime, duration, affPartId);

                    return true;
                }

                case JOB: {
                    if (buf.remaining() < 24 + 8 + 8 + 8 + 1)
                        break;

                    IgniteUuid sesId = readIgniteUuid(buf);
                    long queuedTime = buf.getLong();
                    long startTime = buf.getLong();
                    long duration = buf.getLong();
                    boolean timedOut = buf.get() != 0;

                    for (IgnitePerformanceStatistics handler : handlers)
                        handler.job(sesId, queuedTime, startTime, duration, timedOut);

                    return true;
                }

                default:
                    throw new RuntimeException("Unknown operation type id [typeId=" + opTypeByte + ']');
            }

            buf.position(pos);

            return false;
        }

        /** Reads string from byte buffer. */
        private static String readString(ByteBuffer buf, int size) {
            byte[] bytes = new byte[size];

            buf.get(bytes);

            return new String(bytes);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            stringById.clear();
        }
    }
}
