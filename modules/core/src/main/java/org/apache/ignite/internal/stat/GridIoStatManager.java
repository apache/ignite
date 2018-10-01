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
 *
 */

package org.apache.ignite.internal.stat;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * IO statistics manager.
 */
public class GridIoStatManager {
    /** Map to track physical reads of memory pages by type. */
    private final Map<PageType, LongAdder> TRACK_PHYSICAL_READS = new HashMap<>();

    /** Map to track physical writes of memory pages by type. */
    private final Map<PageType, LongAdder> TRACK_PHYSICAL_WRITES = new HashMap<>();

    /** Map to track physical reads of memory pages by type. */
    private final Map<PageType, LongAdder> TRACK_LOGICAL_READS = new HashMap<>();

    {
        for (PageType pageType : PageType.values()) {
            TRACK_LOGICAL_READS.put(pageType, new LongAdder());
            TRACK_PHYSICAL_READS.put(pageType, new LongAdder());
            TRACK_PHYSICAL_WRITES.put(pageType, new LongAdder());
        }
    }

    /** Time of since statistics start gathering. */
    private volatile LocalDateTime statsSince = LocalDateTime.now();

    /**
     * Reset statistics
     */
    public void resetStats() {
        for (LongAdder value : TRACK_LOGICAL_READS.values())
            value.reset();

        for (LongAdder value : TRACK_PHYSICAL_READS.values())
            value.reset();

        for (LongAdder value : TRACK_PHYSICAL_WRITES.values())
            value.reset();

        statsSince = LocalDateTime.now();
    }

    /**
     * @return When statistics gathering start.
     */
    public LocalDateTime statsSince() {
        return statsSince;
    }

    /**
     * Track physical and logical read of given page.
     *
     * @param pageAddr start address of page.
     */
    public void trackPhysicalAndLogicalRead(long pageAddr) {
        trackByPageAddress(pageAddr, TRACK_PHYSICAL_READS);

        trackByPageAddress(pageAddr, TRACK_LOGICAL_READS);
    }

    /**
     * Track physical write of page.
     *
     * @param pageIoType type of writed page.
     */
    public void trackPhysicalWrite(int pageIoType) {
        trackByPageIoType(pageIoType, TRACK_PHYSICAL_WRITES);
    }

    /**
     * Track logical read of given page.
     *
     * @param pageAddr Address of page.
     */
    public void trackLogicalRead(long pageAddr) {
        trackByPageAddress(pageAddr, TRACK_LOGICAL_READS);
    }

    /**
     * Track of access to given page at given Map.
     *
     * @param pageAddr Address of page.
     * @param mapToTrack Map to track access to the given page.
     */
    private void trackByPageAddress(long pageAddr, Map<PageType, LongAdder> mapToTrack) {
        int pageIoType = PageIO.getType(pageAddr);

        trackByPageIoType(pageIoType, mapToTrack);
    }

    /**
     * Track of access to given page at given Map.
     *
     * @param pageIoType Page IO type.
     * @param mapToTrack Map to track access to page.
     */
    private void trackByPageIoType(int pageIoType, Map<PageType, LongAdder> mapToTrack) {
        if (pageIoType > 0) { // To skip not set type.
            PageType pageType = PageType.derivePageType(pageIoType);

            mapToTrack.get(pageType).increment();
        }
    }

    /**
     * @return Tracked physical reads by types since last reset statistics.
     */
    public Map<PageType, Long> physicalReads() {
        return deepCopyOfStat(TRACK_PHYSICAL_READS);
    }

    /**
     * @return Tracked physical writes by types since last reset statistics.
     */
    public Map<PageType, Long> physicalWrites() {
        return deepCopyOfStat(TRACK_PHYSICAL_WRITES);
    }

    /**
     * @return Tracked logical reads by types since last reset statistics.
     */
    public Map<PageType, Long> logicalReads() {
        return deepCopyOfStat(TRACK_LOGICAL_READS);
    }

    /**
     * @param stat Statistics which need to aggregate.
     * @return Aggregated statistics for given statistics
     */
    public Map<AggregatePageType, AtomicLong> aggregate(Map<PageType, Long> stat) {
        Map<AggregatePageType, AtomicLong> res = new HashMap<>();

        for (AggregatePageType type : AggregatePageType.values())
            res.put(type, new AtomicLong());

        stat.forEach((k, v) -> res.get(AggregatePageType.aggregate(k)).addAndGet(v));

        return res;
    }

    /**
     * @param map Map to create deep copy
     * @return deep copy of statistics.
     */
    private Map<PageType, Long> deepCopyOfStat(Map<PageType, LongAdder> map) {
        Map<PageType, Long> stat = new HashMap<>();

        map.forEach((k,v)-> stat.put(k, v.longValue()));

        return stat;
    }
}
