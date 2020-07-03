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

import java.util.EnumSet;

/**
 * Operation type.
 */
public enum OperationType {
    /** Cache get. */
    CACHE_GET,

    /** Cache put. */
    CACHE_PUT,

    /** Cache remove. */
    CACHE_REMOVE,

    /** Cache get and put. */
    CACHE_GET_AND_PUT,

    /** Cache get and remove. */
    CACHE_GET_AND_REMOVE,

    /** Cache invoke. */
    CACHE_INVOKE,

    /** Cache lock. */
    CACHE_LOCK,

    /** Cache get all. */
    CACHE_GET_ALL,

    /** Cache put all. */
    CACHE_PUT_ALL,

    /** Cache remove all. */
    CACHE_REMOVE_ALL,

    /** Cache invoke all. */
    CACHE_INVOKE_ALL,

    /** Transaction commit. */
    TX_COMMIT,

    /** Transaction rollback. */
    TX_ROLLBACK,

    /** Query. */
    QUERY,

    /** Query reads. */
    QUERY_READS,

    /** Task. */
    TASK,

    /** Job. */
    JOB;

    /** Cache operations. */
    public static final EnumSet<OperationType> CACHE_OPS = EnumSet.range(CACHE_GET, CACHE_INVOKE_ALL);

    /** Transaction operations. */
    public static final EnumSet<OperationType> TX_OPS = EnumSet.of(TX_COMMIT, TX_ROLLBACK);

    /** Values. */
    private static final OperationType[] VALS = values();

    /** @return Operation type from ordinal. */
    public static OperationType fromOrdinal(byte ord) {
        return ord < 0 || ord >= VALS.length ? null : VALS[ord];
    }

    /** @return {@code True} if cache operation. */
    public static boolean cacheOperation(OperationType op) {
        return CACHE_OPS.contains(op);
    }

    /** @return {@code True} if transaction operation. */
    public static boolean transactionOperation(OperationType op) {
        return TX_OPS.contains(op);
    }
}
