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

package org.apache.ignite.internal.profiling.util;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.profiling.IgniteProfiling;
import org.apache.ignite.internal.profiling.IgniteProfiling.CacheOperationType;
import org.apache.ignite.internal.profiling.LogFileProfiling.OperationType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.profiling.LogFileProfiling.readIgniteUuid;
import static org.apache.ignite.internal.profiling.LogFileProfiling.readUuid;

/** */
public class OperationDeserializer {
    /**
     * @param buf Buffer.
     * @param parsers Parsers to notify.
     * @return {@code True} if operation parsed. {@code False} if not enough bytes.
     */
    public static boolean deserialize(ByteBuffer buf, IgniteProfiling... parsers) {
        int pos = buf.position();

        if (buf.remaining() < 1)
            return false;

        byte opTypeByte = buf.get();

        OperationType opType = OperationType.fromOrdinal(opTypeByte);

        switch (opType) {
            case CACHE_OPERATION: {
                if (buf.remaining() < 1 + 4 + 8 + 8)
                    break;

                CacheOperationType cacheOp = CacheOperationType.fromOrdinal(buf.get());
                int cacheId = buf.getInt();
                long startTime = buf.getLong();
                long duration = buf.getLong();

                for (IgniteProfiling parser : parsers)
                    parser.cacheOperation(cacheOp, cacheId, startTime, duration);

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

                for (IgniteProfiling parser : parsers)
                    parser.transaction(cacheIds, startTime, duration, commit);

                return true;
            }

            case QUERY: {
                if (buf.remaining() < 1 + 4)
                    break;

                GridCacheQueryType queryType = GridCacheQueryType.fromOrdinal(buf.get());

                int textLength = buf.getInt();

                if (buf.remaining() < textLength + 16 + 8 + 8 + 8 + 1)
                    break;

                String text = readString(buf, textLength);
                UUID uuid = readUuid(buf);
                long id = buf.getLong();
                long startTime = buf.getLong();
                long duration = buf.getLong();
                boolean success = buf.get() != 0;

                for (IgniteProfiling parser : parsers)
                    parser.query(queryType, text, uuid, id, startTime, duration, success);

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

                for (IgniteProfiling parser : parsers)
                    parser.queryReads(queryType, uuid, id, logicalReads, physicalReads);

                return true;
            }

            case TASK: {
                if (buf.remaining() < 24 + 4)
                    break;

                IgniteUuid sesId = readIgniteUuid(buf);

                int taskNameLength = buf.getInt();

                if (buf.remaining() < taskNameLength + 8 + 8 + 4)
                    break;

                String taskName = readString(buf, taskNameLength);
                long startTime = buf.getLong();
                long duration = buf.getLong();
                int affPartId = buf.getInt();

                for (IgniteProfiling parser : parsers)
                    parser.task(sesId, taskName, startTime, duration, affPartId);

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

                for (IgniteProfiling parser : parsers)
                    parser.job(sesId, queuedTime, startTime, duration, timedOut);

                return true;
            }

            case CACHE_START: {
                if (buf.remaining() < 4 + 8 + 4)
                    break;

                int cacheId = buf.getInt();
                long startTime = buf.getLong();

                int cacheNameLength = buf.getInt();

                if (buf.remaining() < cacheNameLength + 4)
                    break;

                String cacheName = readString(buf, cacheNameLength);

                int groupNameLength = buf.getInt();

                if (buf.remaining() < groupNameLength + 1)
                    break;

                String groupName = readString(buf, groupNameLength);

                boolean userCache = buf.get() != 0;

                for (IgniteProfiling parser : parsers)
                    parser.cacheStart(cacheId, startTime, cacheName, groupName, userCache);

                return true;
            }

            case PROFILING_START: {
                if (buf.remaining() < 20)
                    break;

                UUID nodeId = readUuid(buf);

                int nameLen = buf.getInt();

                if (buf.remaining() < nameLen + 4)
                    break;

                String instanceName = readString(buf, nameLen);

                int verLen = buf.getInt();

                if (buf.remaining() < verLen + 4)
                    break;

                String version = readString(buf, verLen);

                long startTime = buf.getLong();

                for (IgniteProfiling parser : parsers)
                    parser.profilingStart(nodeId, instanceName, version, startTime);

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
}
