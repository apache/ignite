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

package org.apache.ignite.configuration;

import org.jetbrains.annotations.Nullable;

/**
 * Defines memory page eviction algorithm. A mode is set for a specific
 * {@link DataRegionConfiguration}. Only data pages, that store key-value entries, are eligible for eviction. The
 * other types of pages, like index or meta pages, are not evictable.
 */
public enum DataPageEvictionMode {
    /** Eviction is disabled. */
    DISABLED,

    /**
     * Random-LRU algorithm.
     * <ul>
     * <li>Once a memory region defined by a data region is configured, an off-heap array is allocated to track
     * last usage timestamp for every individual data page. The size of the array is calculated this way - size =
     * ({@link DataRegionConfiguration#getMaxSize()} / {@link DataStorageConfiguration#pageSize})</li>
     * <li>When a data page is accessed, its timestamp gets updated in the tracking array. The page index in the
     * tracking array is calculated this way - index = (pageAddress / {@link DataRegionConfiguration#getMaxSize()}</li>
     * <li>When it's required to evict some pages, the algorithm randomly chooses 5 indexes from the tracking array and
     * evicts a page with the latest timestamp. If some of the indexes point to non-data pages (index or system pages)
     * then the algorithm picks other pages.</li>
     * </ul>
     */
    RANDOM_LRU,

    /**
     * Random-2-LRU algorithm: scan-resistant version of Random-LRU.
     * <p>
     * This algorithm differs from Random-LRU only in a way that two latest access timestamps are stored for every
     * data page. At the eviction time, a minimum between two latest timestamps is taken for further comparison with
     * minimums of other pages that might be evicted. LRU-2 outperforms LRU by resolving "one-hit wonder" problem -
     * if a data page is accessed rarely, but accidentally accessed once, it's protected from eviction for a long time.
     */
    RANDOM_2_LRU;

    /** Enumerated values. */
    private static final DataPageEvictionMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static DataPageEvictionMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
