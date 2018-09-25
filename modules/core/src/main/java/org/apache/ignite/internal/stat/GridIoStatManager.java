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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_TRACK_LOG_ENABLE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_TRACK_LOG_TIMEOUT;

/**
 * IO statistics manager.
 */
public class GridIoStatManager {
    /** Map to track physical reads of memmory pages by type. */
    private final ConcurrentHashMap<PageType, LongAdder> TRACK_PHYSICAL_READS = new ConcurrentHashMap<>();

    /** Map to track physical writes of memmory pages by type. */
    private final ConcurrentHashMap<PageType, LongAdder> TRACK_PHYSICAL_WRITES = new ConcurrentHashMap<>();

    /** Map to track physical reads of memmory pages by type. */
    private final ConcurrentHashMap<PageType, LongAdder> TRACK_LOGICAL_READS = new ConcurrentHashMap<>();

    /** Time of since statistics start gathering. */
    private volatile LocalDateTime statsSince;

    /** Enabled or disabled gathering of statistics. */
    private volatile boolean statEnabled = IgniteSystemProperties.getBoolean(IGNITE_PAGE_TRACK_LOG_ENABLE, false);

    /** Period between statistic logging. */
    private final long TRACK_TIMEOUT = IgniteSystemProperties.getLong(IGNITE_PAGE_TRACK_LOG_TIMEOUT, 60000);

    /** Thread to periodical log of IO statistics. */
    private Thread informThread;

    /** Logger. */
    protected IgniteLogger log;

    /**
     * @param igniteLog Ignite logger;
     */
    public GridIoStatManager(IgniteLogger igniteLog) {
        log = igniteLog.getLogger(getClass().getName());

        statEnable(statEnabled);
    }

    /**
     * Enable or disable track statistics
     *
     * @param statEnable {@code true} if statistcis should be tracking
     */
    public synchronized void statEnable(boolean statEnable) {
        this.statEnabled = statEnable;

        if (statEnabled)
            startStatLogThread();
        else {
            stopStatLogThread();

            resetStats();
        }
    }

    /**
     * Reset statistics
     */
    public void resetStats() {
        TRACK_LOGICAL_READS.clear();

        TRACK_PHYSICAL_READS.clear();

        TRACK_PHYSICAL_WRITES.clear();

        statsSince = LocalDateTime.now();
    }

    /**
     * logging statistics.
     */
    public void logStats() {
        GridStringBuilder out = new SB().a("IO stat by types since " + statsSince);

        fillStat(out, TRACK_LOGICAL_READS, "Logical reads");

        fillStat(out, TRACK_PHYSICAL_READS, "Physical reads");

        fillStat(out, TRACK_PHYSICAL_WRITES, "Physical writes");

        if (log.isInfoEnabled())
            log.info(out.toString());
    }

    /**
     * @param out Output builder to fill.
     * @param stat Map contains statistics.
     * @param statName Statistics name.
     */
    private void fillStat(GridStringBuilder out, ConcurrentHashMap<PageType, LongAdder> stat, String statName) {
        out.a('\n').a(statName).a(": ");

        for (Map.Entry<PageType, LongAdder> entry : stat.entrySet()) {
            PageType type = entry.getKey();

            long cnt = entry.getValue().longValue();

            out.a(" ").a(type).a(":").a(cnt);
        }
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
    private void trackByPageAddress(long pageAddr, ConcurrentHashMap<PageType, LongAdder> mapToTrack) {
        int pageIoType = PageIO.getType(pageAddr);

        trackByPageIoType(pageIoType, mapToTrack);
    }

    /**
     * Track of access to given page at given Map.
     *
     * @param pageIoType Page IO type.
     * @param mapToTrack Map to track access to page.
     */
    private void trackByPageIoType(int pageIoType, ConcurrentHashMap<PageType, LongAdder> mapToTrack) {
        if (statEnabled && pageIoType > 0) {
            PageType pageType = PageType.derivePageType(pageIoType);

            mapToTrack.computeIfAbsent(pageType, new Function<PageType, LongAdder>() {
                @Override public LongAdder apply(PageType t) {
                    return new LongAdder();
                }
            }).increment();
        }
    }

    /**
     * Stop statistics logging thread.
     */
    private synchronized void stopStatLogThread() {
        if (informThread != null)
            informThread.interrupt();
    }

    /**
     * Start statistics logging thread. Previous statistics will be reset and previous thread will be interrupted.
     */
    private synchronized void startStatLogThread() {
        stopStatLogThread();

        resetStats();

        informThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (true) {
                        Thread.sleep(TRACK_TIMEOUT);

                        logStats();

                        resetStats();
                    }
                }
                catch (Exception e) {
                    // No-op.
                }
            }
        });

        informThread.setName("io-stats-tracker");
        informThread.setDaemon(true);

        informThread.start();
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
     * @param map Map to create deep copy
     * @return deep copy of statistics.
     */
    private Map<PageType, Long> deepCopyOfStat(Map<PageType, LongAdder> map) {
        Map<PageType, Long> stat = new HashMap<>();

        for (Map.Entry<PageType, LongAdder> entry : map.entrySet()) {
            stat.put(entry.getKey(), entry.getValue().longValue());
        }

        return stat;
    }

    /** */
    public enum PageType {
        /** Pages related to H2. */
        H2(
            range(PageIO.T_H2_REF_LEAF),
            range(PageIO.T_H2_REF_INNER),
            range(PageIO.T_H2_MVCC_REF_LEAF),
            range(PageIO.T_H2_MVCC_REF_INNER),
            range(PageIO.T_H2_EX_REF_LEAF_START, PageIO.T_H2_EX_REF_LEAF_END),
            range(PageIO.T_H2_EX_REF_INNER_START, PageIO.T_H2_EX_REF_INNER_END),
            range(PageIO.T_H2_EX_REF_MVCC_LEAF_START, PageIO.T_H2_EX_REF_MVCC_LEAF_END),
            range(PageIO.T_H2_EX_REF_MVCC_INNER_START, PageIO.T_H2_EX_REF_MVCC_INNER_END)
        ),

        /** Data pages. */
        DATA(
            range(PageIO.T_DATA),
            range(PageIO.T_DATA_REF_INNER),
            range(PageIO.T_DATA_REF_LEAF),
            range(PageIO.T_CACHE_ID_AWARE_DATA_REF_INNER),
            range(PageIO.T_CACHE_ID_AWARE_DATA_REF_LEAF),
            range(PageIO.T_DATA_METASTORAGE),
            range(PageIO.T_DATA_REF_METASTORAGE_INNER),
            range(PageIO.T_DATA_REF_METASTORAGE_LEAF),
            range(PageIO.T_DATA_REF_MVCC_INNER),
            range(PageIO.T_DATA_REF_MVCC_LEAF),
            range(PageIO.T_CACHE_ID_DATA_REF_MVCC_INNER),
            range(PageIO.T_CACHE_ID_DATA_REF_MVCC_LEAF)
        ),
        /** */
        OTHER;

        /** Related to {@code pageType} pageIo types. */
        private final Set<T2<Integer, Integer>> pageIoTypesRange;

        /**
         * Create range with single elment.
         *
         * @param singleVal Value for range.
         * @return Created range.
         */
        private static T2<Integer, Integer> range(int singleVal) {
            return range(singleVal, singleVal);
        }

        /**
         * Create range for given arguments
         *
         * @param leftBound Left bound of range.
         * @param rightBound Right bound of range.
         * @return Created range.
         */
        private static T2<Integer, Integer> range(int leftBound, int rightBound) {
            return new T2<>(leftBound, rightBound);
        }

        /**
         * @param typeRanges Page IO range of types related to constructing of page type.
         */
        PageType(T2<Integer, Integer>... typeRanges) {
            HashSet<T2<Integer, Integer>> set = new HashSet<>();

            for (T2<Integer, Integer> t2 : typeRanges) {
                assert t2.get1() <= t2.get2();

                set.add(t2);
            }

            pageIoTypesRange = Collections.unmodifiableSet(set);
        }

        /**
         * Convert page io type to {@code PageType}.
         *
         * @param pageIoType Page io type.
         * @return Derived PageType.
         */
        static PageType derivePageType(int pageIoType) {
            for (PageType value : values()) {
                for (T2<Integer, Integer> range : value.pageIoTypesRange) {
                    if (range.get1() <= pageIoType && pageIoType <= range.get2())
                        return value;
                }
            }

            return OTHER;
        }
    }
}
