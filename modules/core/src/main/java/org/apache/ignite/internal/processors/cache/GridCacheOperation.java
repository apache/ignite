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

package org.apache.ignite.internal.processors.cache;

import org.jetbrains.annotations.Nullable;

/**
 * Cache value operations.
 */
public enum GridCacheOperation {
    /** Read operation. */
    READ,

    /** Create operation. */
    CREATE,

    /** Update operation. */
    UPDATE,

    /** Delete operation. */
    DELETE,

    /** Transform operation. A closure will be applied to the previous entry value. */
    TRANSFORM,

    /** Cache operation used to indicate reload during transaction recovery. */
    RELOAD,

    /**
     * This operation is used when lock has been acquired,
     * but filter validation failed.
     */
    NOOP;

    /** Enum values. */
    private static final GridCacheOperation[] VALS = values();

    /**
     * @param ord Ordinal value.
     * @return Enum value.
     */
    @Nullable public static GridCacheOperation fromOrdinal(int ord) {
        return ord < 0 || ord >= VALS.length ? null : VALS[ord];
    }

    /** */
    public static short encode(GridCacheOperation operation) {
        switch (operation) {
            case READ: return 1;
            case CREATE: return 2;
            case UPDATE: return 3;
            case DELETE: return 4;
            case TRANSFORM: return 5;
            case RELOAD: return 6;
            case NOOP: return 7;
        }

        throw new UnsupportedOperationException("Unsupported cache operation: " + operation);
    }

    /** */
    public static GridCacheOperation decode(short operationCode) {
        switch (operationCode) {
            case 1: return READ;
            case 2: return CREATE;
            case 3: return UPDATE;
            case 4: return DELETE;
            case 5: return TRANSFORM;
            case 6: return RELOAD;
            case 7: return NOOP;
        }

        throw new UnsupportedOperationException("Unsupported cache operation code: " + operationCode);
    }
}
