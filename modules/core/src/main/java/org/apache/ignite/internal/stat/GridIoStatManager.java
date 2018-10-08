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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * IO statistics manager.
 */
public class GridIoStatManager {
    /** Context to gathering specific IO statistics. */
    private static final ThreadLocal<List<StatOperationType>> currentOperationType = ThreadLocal.withInitial(ArrayList::new);

    /** Complex map to track physical reads of memory pages */
    private final Map<StatType, Map<PageType, Map<Object, LongAdder>>> TRACK_PHYSICAL_READS = new EnumMap<>(StatType.class);

    /** Complex map to track physical writes of memory pages */
    private final Map<StatType, Map<PageType, Map<Object, LongAdder>>> TRACK_PHYSICAL_WRITES = new EnumMap<>(StatType.class);

    /** Complex map to track logical reads of memory pages */
    private final Map<StatType, Map<PageType, Map<Object, LongAdder>>> TRACK_LOGICAL_READS = new EnumMap<>(StatType.class);

    {
        for (StatType statType : StatType.values()) {
            Map<PageType, Map<Object, LongAdder>> logReadMap = new EnumMap<>(PageType.class);

            Map<PageType, Map<Object, LongAdder>> physReadMap = new EnumMap<>(PageType.class);

            Map<PageType, Map<Object, LongAdder>> physWriteMap = new EnumMap<>(PageType.class);

            for (PageType pageType : PageType.values()) {
                if (statType == StatType.GLOBAL) {
                    logReadMap.put(pageType, new HashMap<>(2));

                    physReadMap.put(pageType, new HashMap<>(2));

                    physWriteMap.put(pageType, new HashMap<>(2));

                    //Predefined values for global statistics.
                    {
                        logReadMap.get(pageType).put(KEY_FOR_GLOBAL_STAT, new LongAdder());

                        physReadMap.get(pageType).put(KEY_FOR_GLOBAL_STAT, new LongAdder());

                        physWriteMap.get(pageType).put(KEY_FOR_GLOBAL_STAT, new LongAdder());
                    }
                }
                else {
                    logReadMap.put(pageType, new ConcurrentHashMap<>());

                    physReadMap.put(pageType, new ConcurrentHashMap<>());

                    physWriteMap.put(pageType, new ConcurrentHashMap<>());
                }
            }

            TRACK_LOGICAL_READS.put(statType, logReadMap);

            TRACK_PHYSICAL_READS.put(statType, physReadMap);

            TRACK_PHYSICAL_WRITES.put(statType, physWriteMap);
        }
    }

    /** Key for GLOBAL statistics. */
    public static Object KEY_FOR_GLOBAL_STAT = StatType.GLOBAL;

    /** Time of since statistics start gathering. */
    private volatile LocalDateTime statsSince = LocalDateTime.now();

    /**
     * Add operation types statistics context for current thread.
     *
     * @param statTypes Statistic operation types.
     */
    public static void addCurrentOperationType(StatOperationType... statTypes) {
        for (StatOperationType statType : statTypes) {
            currentOperationType.get().add(statType);
        }
    }

    /**
     * Remove operation types statistics context for current thread.
     *
     * @param statTypes Statistic operation types.
     */
    public static void removeCurrentOperationType(StatOperationType... statTypes) {
        for (StatOperationType statType : statTypes) {
            currentOperationType.get().remove(statType);
        }
    }

    /**
     * Reset statistics
     */
    public void resetStats() {
        Stream.of(TRACK_LOGICAL_READS, TRACK_PHYSICAL_READS, TRACK_PHYSICAL_WRITES).forEach(stat -> {
                for (Map.Entry<StatType, Map<PageType, Map<Object, LongAdder>>> entry : stat.entrySet()) {
                    if (entry.getKey() == StatType.GLOBAL) {
                        for (Map<Object, LongAdder> value : entry.getValue().values())
                            value.get(KEY_FOR_GLOBAL_STAT).reset();
                    }
                    else {
                        for (Map<Object, LongAdder> value : entry.getValue().values())
                            value.clear();
                    }
                }
            }
        );

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
    private void trackByPageAddress(long pageAddr, Map<StatType, Map<PageType, Map<Object, LongAdder>>> mapToTrack) {
        int pageIoType = PageIO.getType(pageAddr);

        trackByPageIoType(pageIoType, mapToTrack);
    }

    /**
     * Track of access to given page at given Map.
     *
     * @param pageIoType Page IO type.
     * @param mapToTrack Map to track access to page.
     */
    private void trackByPageIoType(int pageIoType, Map<StatType, Map<PageType, Map<Object, LongAdder>>> mapToTrack) {
        if (pageIoType > 0) { // To skip not set type.
            PageType pageType = PageType.derivePageType(pageIoType);

            mapToTrack.get(StatType.GLOBAL).get(pageType).get(KEY_FOR_GLOBAL_STAT).increment();

            List<StatOperationType> statOpTypes = currentOperationType.get();

            if (!statOpTypes.isEmpty())
                for (StatOperationType opType : statOpTypes)
                    mapToTrack.get(opType.type()).get(pageType)
                        .computeIfAbsent(opType.subType(), k -> new LongAdder())
                        .increment();
        }
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
     * @return Tracked physical reads by types since last reset statistics.
     */
    public Map<PageType, Long> physicalReadsGlobal() {
        return extractStat(TRACK_PHYSICAL_READS, StatType.GLOBAL, KEY_FOR_GLOBAL_STAT);
    }

    /**
     * @return Tracked physical writes by types since last reset statistics.
     */
    public Map<PageType, Long> physicalWritesGlobal() {
        return extractStat(TRACK_PHYSICAL_WRITES, StatType.GLOBAL, KEY_FOR_GLOBAL_STAT);
    }

    /**
     * @return Tracked global logical reads by types since last reset statistics.
     */
    public Map<PageType, Long> logicalReadsGlobal() {
        return extractStat(TRACK_LOGICAL_READS, StatType.GLOBAL, KEY_FOR_GLOBAL_STAT);
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param subType Subtype of statistics which need to take.
     * @return Tracked logical reads by types since last reset statistics.
     */
    public Map<PageType, Long> logicalReads(StatType statType, Object subType) {
        return extractStat(TRACK_LOGICAL_READS, statType, subType);
    }

    /**
     * @param statType Type of statistics which need to take.
     * @param subType Subtype of statistics which need to take.
     * @return Tracked phisycal reads by types since last reset statistics.
     */
    public Map<PageType, Long> physicalReads(StatType statType, Object subType) {
        return extractStat(TRACK_PHYSICAL_READS, statType, subType);
    }

    /**
     * Extract all tracked logical reads subtypes.
     *
     * @param statType Type of statistics which subtypes need to extract.
     * @param <T> Type of subTypes.
     * @return Set of present subtypes for given statType.
     */
    public <T> Set<T> subTypesLogicalReads(StatType statType) {
        return extractSubTypes(TRACK_LOGICAL_READS, statType);
    }

    /**
     * Extract all tracked physical reads subtypes.
     *
     * @param statType Type of statistics which subtypes need to extract.
     * @param <T> Type of subTypes.
     * @return Set of present subtypes for given statType.
     */
    public <T> Set<T> subTypesPhysicalReads(StatType statType) {
        return extractSubTypes(TRACK_PHYSICAL_READS, statType);
    }

    /**
     * Extract all present subtypes.
     *
     * @param statMap Map with full statistics.
     * @param statType Type of statistics which subtypes need to extract.
     * @return Set of present subtypes for given statType
     * @param <T> Type of subtype.
     */
    @SuppressWarnings({"unchecked"})
    private <T> Set<T> extractSubTypes(Map<StatType, Map<PageType, Map<Object, LongAdder>>> statMap,
        StatType statType) {
        assert statType != null;

        Set<Object> res = new HashSet<>();

        for (Map<Object, LongAdder> value : statMap.get(statType).values())
            res.addAll(value.keySet());

        return (Set<T>)res;
    }

    /**
     * Extract statistics.
     *
     * @param statMap Map with full statistics.
     * @param statType Type of statistics which need to be extracted.
     * @param subtype Subtype of statistics which need to be extracted.
     * @return Extracted statistics.
     */
    private Map<PageType, Long> extractStat(Map<StatType, Map<PageType, Map<Object, LongAdder>>> statMap,
        StatType statType, Object subtype) {
        Map<PageType, Long> stat = new HashMap<>();

        for (Map.Entry<PageType, Map<Object, LongAdder>> entry : statMap.get(statType).entrySet()) {
            LongAdder cntr = entry.getValue().get(subtype);

            if (cntr != null)
                stat.put(entry.getKey(), cntr.longValue());
        }

        return stat;
    }
}
