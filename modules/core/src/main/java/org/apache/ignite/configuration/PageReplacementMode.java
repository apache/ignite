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

import org.apache.ignite.DataRegionMetrics;
import org.jetbrains.annotations.Nullable;

/**
 * Defines memory page replacement algorithm. A mode is set for a specific {@link DataRegionConfiguration}.
 */
public enum PageReplacementMode {
    /**
     * Random-LRU algorithm.
     *
     * Every time page is accessed, its timestamp gets updated. When a page fault occurs and it's required to replace
     * some pages, the algorithm randomly chooses 5 pages from the page memory and evicts a page with the latest
     * timestamp.
     *
     * This algorithm has zero maintenance cost, but not very effective in terms of finding the next page to replace.
     * Recommended to use in environments, where there are no page replacements (large enough data region to store all
     * amount of data) or page replacements are rare (See {@link DataRegionMetrics#getPagesReplaceRate()} metric, which
     * can be helpful here).
     */
    RANDOM_LRU,

    /**
     * Segmented-LRU algorithm.
     *
     * Segmented-LRU algorithm is a scan-resistant variation of the Least Recently Used (LRU) algorithm.
     * Segmented-LRU pages list is divided into two segments, a probationary segment, and a protected segment. Pages in
     * each segment are ordered from the least to the most recently accessed. New pages are added to the most recently
     * accessed end (tail) of the probationary segment. Existing pages are removed from wherever they currently reside
     * and added to the most recently accessed end of the protected segment. Pages in the protected segment have thus
     * been accessed at least twice. The protected segment is finite, so migration of a page from the probationary
     * segment to the protected segment may force the migration of the LRU page in the protected segment to the most
     * recently used end of the probationary segment, giving this page another chance to be accessed before being
     * replaced. Page to replace is polled from the least recently accessed end (head) of the probationary segment.
     *
     * This algorithm requires additional memory to store pages list and need to update this list on each page access,
     * but have near to optimal page to replace selection policy. So, there can be a little performance drop for
     * environments without page replacement (compared to random-LRU and CLOCK), but for environments with a high rate
     * of page replacement and a large amount of one-time scans segmented-LRU can outperform random-LRU and CLOCK.
     */
    SEGMENTED_LRU,

    /**
     * CLOCK algorithm.
     *
     * The clock algorithm keeps a circular list of pages in memory, with the "hand" pointing to the last examined page
     * frame in the list. When a page fault occurs and no empty frames exist, then the hit flag of the page is inspected
     * at the hand's location. If the hit flag is 0, the new page is put in place of the page the "hand" points to, and
     * the hand is advanced one position. Otherwise, the hit flag is cleared, then the clock hand is incremented and the
     * process is repeated until a page is replaced.
     *
     * This algorithm has near to zero maintenance cost and replacement policy efficiency between random-LRU and
     * segmented-LRU.
     */
    CLOCK;

    /** Enumerated values. */
    private static final PageReplacementMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static PageReplacementMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
