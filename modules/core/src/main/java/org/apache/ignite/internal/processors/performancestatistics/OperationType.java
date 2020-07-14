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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Performance statistics operation type.
 */
public enum OperationType {
    /** Cache get. */
    CACHE_GET(0),

    /** Cache put. */
    CACHE_PUT(1),

    /** Cache remove. */
    CACHE_REMOVE(2),

    /** Cache get and put. */
    CACHE_GET_AND_PUT(3),

    /** Cache get and remove. */
    CACHE_GET_AND_REMOVE(4),

    /** Cache invoke. */
    CACHE_INVOKE(5),

    /** Cache lock. */
    CACHE_LOCK(6),

    /** Cache get all. */
    CACHE_GET_ALL(7),

    /** Cache put all. */
    CACHE_PUT_ALL(8),

    /** Cache remove all. */
    CACHE_REMOVE_ALL(9),

    /** Cache invoke all. */
    CACHE_INVOKE_ALL(10),

    /** Transaction commit. */
    TX_COMMIT(11),

    /** Transaction rollback. */
    TX_ROLLBACK(12),

    /** Query. */
    QUERY(13),

    /** Query reads. */
    QUERY_READS(14),

    /** Task. */
    TASK(15),

    /** Job. */
    JOB(16);

    /** Cache operations. */
    public static final EnumSet<OperationType> CACHE_OPS = EnumSet.of(CACHE_GET, CACHE_PUT, CACHE_REMOVE,
        CACHE_GET_AND_PUT, CACHE_GET_AND_REMOVE, CACHE_INVOKE, CACHE_LOCK, CACHE_GET_ALL, CACHE_PUT_ALL,
        CACHE_REMOVE_ALL, CACHE_INVOKE_ALL);

    /** Transaction operations. */
    public static final EnumSet<OperationType> TX_OPS = EnumSet.of(TX_COMMIT, TX_ROLLBACK);

    /** Value by identifier. */
    private static final Map<Byte, OperationType> VALS;

    /** Unique operation identifier. */
    private final byte id;

    /** Static initializer. */
    static {
        Map<Byte, OperationType> vals = new HashMap<>();

        for (OperationType op : values()) {
            OperationType old = vals.put(op.id(), op);

            assert old == null : "Duplicate operation ID found [op=" + op + ']';
        }

        VALS = Collections.unmodifiableMap(vals);
    }

    /** @param id Unique operation identifier. */
    OperationType(int id) {
        this.id = (byte)id;
    }

    /** @return Unique operation identifier. */
    public byte id() {
        return id;
    }

    /** @return Operation type of given identifier. */
    @Nullable public static OperationType of(byte id) {
        return VALS.get(id);
    }

    /** @return {@code True} if cache operation. */
    public static boolean cacheOperation(OperationType op) {
        return CACHE_OPS.contains(op);
    }

    /** @return {@code True} if transaction operation. */
    public static boolean transactionOperation(OperationType op) {
        return TX_OPS.contains(op);
    }

    /** @return Cache record size. */
    public static int cacheRecordSize() {
        return 4 + 8 + 8;
    }

    /**
     * @param cachesIdsCnt Cache identifiers size.
     * @return Transaction record size.
     */
    public static int transactionRecordSize(int cachesIdsCnt) {
        return 4 + cachesIdsCnt * 4 + 8 + 8;
    }

    /**
     * @param textLen Query text length.
     * @return Query record size.
     */
    public static int queryRecordSize(int textLen) {
        return 4 + textLen + 1 + 8 + 8 + 8 + 1;
    }

    /** @return Query reads record size. */
    public static int queryReadsRecordSize() {
        return 1 + 16 + 8 + 8 + 8;
    }

    /**
     * @param nameLen Task name length.
     * @return Task record size.
     */
    public static int taskRecordSize(int nameLen) {
        return 4 + nameLen + 24 + 8 + 8 + 4;
    }

    /** @return Job record size. */
    public static int jobRecordSize() {
        return 24 + 8 + 8 + 8 + 1;
    }
}
