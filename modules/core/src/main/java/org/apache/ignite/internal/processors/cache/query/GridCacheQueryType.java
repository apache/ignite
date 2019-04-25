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

package org.apache.ignite.internal.processors.cache.query;

import org.jetbrains.annotations.Nullable;

/**
 * Defines different cache query types. For more information on cache query types
 * and their usage see {@link CacheQuery} documentation.
 * @see CacheQuery
 */
public enum GridCacheQueryType {
    /**
     * User provided indexing SPI based query.
     */
    SPI,

    /**
     * Fully scans cache returning only entries that pass certain filters.
     */
    SCAN,

    /**
     * SQL-based query.
     */
    SQL,

    /**
     * SQL-based fields query.
     */
    SQL_FIELDS,

    /**
     * Text search query.
     */
    TEXT,

    /**
     * Cache set items query.
     */
    SET;

    /** Enumerated values. */
    private static final GridCacheQueryType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridCacheQueryType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}