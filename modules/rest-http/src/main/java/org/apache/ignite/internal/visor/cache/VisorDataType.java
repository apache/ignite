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

package org.apache.ignite.internal.visor.cache;

import org.jetbrains.annotations.Nullable;

/**
 * Enumeration of all supported cache key types in {@link VisorCacheGetValueTask}.
 */
public enum VisorDataType {
    /**  */
    STRING,

    /**  */
    CHARACTER,

    /**  */
    BYTE,

    /**  */
    SHORT,

    /**  */
    INT,

    /**  */
    LONG,

    /**  */
    FLOAT,

    /**  */
    DOUBLE,

    /**  */
    BOOLEAN,

    /**  */
    TIMESTAMP,

    /**  */
    DATE_UTIL,

    /**  */
    DATE_SQL,

    /**  */
    INSTANT,

    /**  */
    UUID,

    /**  */
    BINARY,

    /**  */
    BIG_INTEGER,

    /**  */
    BIG_DECIMAL;

    /** Enumerated values. */
    private static final VisorDataType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static VisorDataType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
