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
package org.apache.ignite;


/**
 * This interface provides calculated metrics for data region.
 */
public interface DataRegionMetricsProvider {
    /**
     * Calculates free space of partially filled pages for this data region. It does not include
     * empty data pages.
     *
     * @return free space in bytes.
     */
    public long partiallyFilledPagesFreeSpace();

    /**
     * Calculates empty data pages count for region. It counts only totally free pages that
     * can be reused (e. g. pages that are contained in reuse bucket of free list).
     *
     * @return empty data pages count.
     */
    public long emptyDataPages();
}
