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
package org.apache.ignite.development.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

public class TxWalStat {
    public static final int DISPLAY_MAX = WalStat.DISPLAY_MAX;
    /** Opened. */
    private Map<GridCacheVersion, TxOccurrence> opened = new HashMap<>();

    private RecordSizeCountStat avgPrimaryNodes = new RecordSizeCountStat();
    private RecordSizeCountStat avgTotalNodes = new RecordSizeCountStat();

    /** Tx: Entries updated */
    private final Map<Integer, Integer> txEntriesUpdated = new TreeMap<>();

    /** Tx: Caches Involved */
    private final Map<Integer, Integer> txCachesInvolved = new TreeMap<>();

    public static final int POPULAR_MAX = 1000;
    public static final int USAGES_CNT_EVICT_PROHIBITED = 10;

    private LruMap<String> cacheIdsInTx = new LruMap<>(POPULAR_MAX, USAGES_CNT_EVICT_PROHIBITED);

    private LruMap<String> cacheIdsWeightedNodesInTx = new LruMap<>(POPULAR_MAX,
        USAGES_CNT_EVICT_PROHIBITED * 90);

    private LruMap<String> cacheIdsWeightedTotalNodesInTx = new LruMap<>(POPULAR_MAX,
        USAGES_CNT_EVICT_PROHIBITED * 150);

    private <K> void computeStatIfAbsent(K key, Map<K, Integer> map) {
        computeStatIfAbsent(key, map, 1);
    }

    private <K> void computeStatIfAbsent(K key, Map<K, Integer> map, int increment) {
        final Integer val = map.get(key);
        int recordStat = val == null ? 0 : val;
        recordStat += increment;
        map.put(key, recordStat);
    }

    public void start(GridCacheVersion nearXidVer, int nodes, int totalNodes) {
        computeIfAbsent(nearXidVer, nodes, totalNodes);
    }

    /**
     * @param nearXidVer
     * @param nodes
     * @param totalNodes
     * @return
     */
    private TxOccurrence computeIfAbsent(GridCacheVersion nearXidVer, int nodes, int totalNodes) {
        TxOccurrence occurrence = opened.get(nearXidVer);
        if (occurrence == null)
            occurrence = new TxOccurrence(nodes, totalNodes);

        opened.put(nearXidVer, occurrence);
        return occurrence;
    }

    /**
     * @param nearXidVer
     * @param commit
     */
    public void close(GridCacheVersion nearXidVer, boolean commit) {
        TxOccurrence occurrence = opened.remove(nearXidVer);
        if (occurrence == null)
            return;

        if (!commit)
            return;

        if (occurrence.nodes > 0 && occurrence.totalNodes > 0) {
            avgPrimaryNodes.occurrence(occurrence.nodes);
            avgTotalNodes.occurrence(occurrence.totalNodes);
        }

        computeStatIfAbsent(occurrence.entriesUpdated, txEntriesUpdated);

        computeStatIfAbsent(occurrence.caches.size(), txCachesInvolved);

        if (!occurrence.caches.isEmpty()) {
            final String sortedCachesKey = occurrence.caches.toString();

            computeStatIfAbsent(sortedCachesKey, cacheIdsInTx.map, 1);

            if (occurrence.nodes > 0)
                computeStatIfAbsent(sortedCachesKey, cacheIdsWeightedNodesInTx.map, occurrence.nodes);

            if (occurrence.totalNodes > 0)
                computeStatIfAbsent(sortedCachesKey, cacheIdsWeightedTotalNodesInTx.map, occurrence.totalNodes);
        }
    }

    public void entry(DataEntry next) {
        if (next.nearXidVersion() == null)
            return;

        computeIfAbsent(next.nearXidVersion(), -1, -1).entry(next);
    }

    /**
     * @param sb
     * @param mapName
     * @param map
     */
    private void printSizeCountMap(StringBuilder sb, String mapName, Map<?, Integer> map) {
        sb.append(mapName).append(": \n");
        sb.append("key\tcount");
        sb.append("\n");

        final List<? extends Map.Entry<?, Integer>> entries = new ArrayList<>(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<?, Integer>>() {
            @Override public int compare(Map.Entry<?, Integer> o1, Map.Entry<?, Integer> o2) {
                return -Integer.compare(o1.getValue(), o2.getValue());
            }
        });

        int othersCnt = 0;
        int othersSum = 0;
        int cnt = 0;
        for (Map.Entry<?, Integer> next : entries) {
            if (cnt < DISPLAY_MAX) {
                sb.append(next.getKey()).append("\t").append(next.getValue()).append("\t");
                sb.append("\n");
            }
            else {
                othersCnt++;
                othersSum += next.getValue();
            }
            cnt++;
        }
        if (othersCnt > 0) {
            sb.append("... other ").append(othersCnt).append(" values").append("\t").append(othersSum).append("\t");
            sb.append("\n");
        }
        sb.append("\n");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Tx stat: remained Opened: \t").append(opened.size()).append("\n").append("\n");

        sb.append("Tx stat: Average Primary Node Count: \t").append(avgPrimaryNodes.averageStr()).append("\n").append("\n");

        sb.append("Tx stat: Average Total Node Count: \t").append(avgTotalNodes.averageStr()).append("\n").append("\n");

        printSizeCountMap(sb, "Tx stat: Entries updated", txEntriesUpdated);
        printSizeCountMap(sb, "Tx stat: Caches involved", txCachesInvolved);
        printSizeCountMap(sb, "Tx stat: Caches list in TX, evicted = " + cacheIdsInTx.evicted, cacheIdsInTx.map);

        printSizeCountMap(sb, "Tx stat: Caches list in TX; weighted by primary Nodes, evicted = "
            + cacheIdsWeightedNodesInTx.evicted, cacheIdsWeightedNodesInTx.map);

        printSizeCountMap(sb, "Tx stat: Caches list in TX; weighted by total Nodes, evicted = "
            + cacheIdsWeightedNodesInTx.evicted, cacheIdsWeightedNodesInTx.map);
        return sb.toString();
    }

    private class TxOccurrence {
        private int nodes;

        private int entriesUpdated;
        /** Caches. */
        private TreeSet<Integer> caches = new TreeSet<>();
        public int totalNodes;

        public TxOccurrence(int nodes, int totalNodes) {
            this.nodes = nodes;
            this.totalNodes = totalNodes;
        }

        /**
         * @param nodes
         */
        public void setNodes(int nodes) {
            this.nodes = nodes;
        }

        /**
         * @param next
         */
        public void entry(DataEntry next) {
            entriesUpdated++;

            caches.add(next.cacheId());
        }
    }

    private class LruMap<K> {
        private int maxSize;
        private int evictProhibited;
        private int evicted;

        LinkedHashMap<K, Integer> map = new LinkedHashMap<K, Integer>(16, 0.75F, false) {
            @Override protected boolean removeEldestEntry(Map.Entry<K, Integer> eldest) {
                if (size() < maxSize)
                    return false;

                final boolean evictNow = eldest.getValue() < evictProhibited;

                if (evictNow)
                    evicted++;

                return evictNow;
            }
        };

        public LruMap(int maxSize, int evictProhibited) {
            this.maxSize = maxSize;
            this.evictProhibited = evictProhibited;
        }
    }
}
