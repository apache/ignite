/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
}