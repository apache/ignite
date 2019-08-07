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

/**
 * Transactions statistics for WAL.
 */
public class TxWalStat {
    /** Maximum element in popular combinations discovery map before eviction. */
    private static final int POPULAR_COMBINATION_MAP_MAX_SIZE = 50000;

    /** Usages count, when evict prohibited from popular combinations discovery map. */
    private static final int USAGES_CNT_EVICT_PROHIBITED = 10;

    /** Transactions in preparing/committing state. When commit is finished, TX is removed from this collection. */
    private final Map<GridCacheVersion, TxOccurrence> opened = new HashMap<>();

    /** Field for calculating average number of primary nodes involved in Tx. */
    private final RecordSizeCountStat avgPrimaryNodes = new RecordSizeCountStat();

    /** Field for calculating average number of total nodes involved in Tx. */
    private final RecordSizeCountStat avgTotalNodes = new RecordSizeCountStat();

    /** Tx statistics: Entries updated count -> count of such Txes. */
    private final Map<Integer, Integer> txEntriesUpdated = new TreeMap<>();

    /** Tx statistics: Caches Involved  count -> count of such Txes.  */
    private final Map<Integer, Integer> txCachesInvolved = new TreeMap<>();

    /** Cache IDs combination involved in Tx. Popular combination search map, with possible eviction. */
    private final LruMap<String> cacheIdsInTx = new LruMap<>(POPULAR_COMBINATION_MAP_MAX_SIZE,
        USAGES_CNT_EVICT_PROHIBITED);

    /**
     * Cache IDs combination involved in Tx, weighted using primary nodes in Tx. Used to search popular combinations
     * mostly involved into highly distributive transactions.
     */
    private final LruMap<String> cacheIdsWeightedNodesInTx = new LruMap<>(POPULAR_COMBINATION_MAP_MAX_SIZE,
        USAGES_CNT_EVICT_PROHIBITED * 90);

    /**
     * Cache IDs combination involved in Tx, weighted using total nodes in Tx. Used to search popular combinations
     * mostly involved into highly distributive transactions.
     */
    private final LruMap<String> cacheIdsWeightedTotalNodesInTx = new LruMap<>(POPULAR_COMBINATION_MAP_MAX_SIZE,
        USAGES_CNT_EVICT_PROHIBITED * 150);

    /**
     * @param key key (parameter) value found.
     * @param map map to save increment.
     * @param <K> type of key.
     */
    private static <K> void incrementStat(K key, Map<K, Integer> map) {
        incrementStat(key, map, 1);
    }

    /**
     * @param key key (parameter) value found.
     * @param map map to save increment.
     * @param increment value to increment statistic, 1 or weight of current occurrence.
     * @param <K> type of key.
     */
    private static <K> void incrementStat(K key, Map<K, Integer> map, int increment) {
        Integer val = map.get(key);
        int recordStat = val == null ? 0 : val;

        recordStat += increment;
        map.put(key, recordStat);
    }

    /**
     * Handles TX prepare: creates TX in {@link #opened} map.
     * @param nearXidVer near Xid Version. Global transaction identifier within cluster, assigned by transaction
     * coordinator.
     * @param nodes primary nodes registered in prepare record.
     * @param totalNodes all nodes (primary & backup) in prepare record.
     */
    void onTxPrepareStart(GridCacheVersion nearXidVer, int nodes, int totalNodes) {
        txComputeIfAbsent(nearXidVer, nodes, totalNodes);
    }

    /**
     * @param nearXidVer near Xid Version. Global transaction identifier within cluster, assigned by transaction
     * coordinator.
     * @param nodes primary nodes registered in prepare record.
     * @param totalNodes all nodes (primary & backup) in prepare record.
     * @return tx occurrence to accumulate entries into.
     */
    private TxOccurrence txComputeIfAbsent(GridCacheVersion nearXidVer, int nodes, int totalNodes) {
        TxOccurrence occurrence = opened.get(nearXidVer);

        if (occurrence == null)
            occurrence = new TxOccurrence(nodes, totalNodes);

        opened.put(nearXidVer, occurrence);

        return occurrence;
    }

    /**
     * Handles commit or rollback transaction. Finished statistics accumulation.
     *
     * @param nearXidVer near Xid Version. Global transaction identifier within cluster, assigned by transaction
     * coordinator.
     * @param commit tx committed, flag indicating TX successes.
     */
    void onTxEnd(GridCacheVersion nearXidVer, boolean commit) {
        TxOccurrence occurrence = opened.remove(nearXidVer);

        if (occurrence == null)
            return;

        if (!commit)
            return;

        if (occurrence.nodes > 0 && occurrence.totalNodes > 0) {
            avgPrimaryNodes.occurrence(occurrence.nodes);
            avgTotalNodes.occurrence(occurrence.totalNodes);
        }

        incrementStat(occurrence.entriesUpdated, txEntriesUpdated);

        incrementStat(occurrence.caches.size(), txCachesInvolved);

        if (!occurrence.caches.isEmpty()) {
            final String sortedCachesKey = occurrence.caches.toString();

            incrementStat(sortedCachesKey, cacheIdsInTx.map, 1);

            if (occurrence.nodes > 0)
                incrementStat(sortedCachesKey, cacheIdsWeightedNodesInTx.map, occurrence.nodes);

            if (occurrence.totalNodes > 0)
                incrementStat(sortedCachesKey, cacheIdsWeightedTotalNodesInTx.map, occurrence.totalNodes);
        }
    }

    /**
     * Handles Data entry from data record. Entries not under transaction are ignored.
     *
     * @param entry object updated.
     */
    void onDataEntry(DataEntry entry) {
        final GridCacheVersion ver = entry.nearXidVersion();

        if (ver == null)
            return;

        txComputeIfAbsent(ver, -1, -1).onDataEntry(entry);
    }

    /**
     * @param sb buffer.
     * @param mapName display name of map.
     * @param map values.
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
            if (cnt < WalStat.DISPLAY_MAX) {
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

    /**
     * Tx in prepare or in commit state, used to accumulate statistic.
     */
    private static class TxOccurrence {
        /** Primary nodes count from TX record. */
        private int nodes;

        /** Primary + backup nodes count from TX record. */
        private int totalNodes;

        /** Count of entries updated under current transaction on current node. */
        private int entriesUpdated;

        /** Sorted set of cache IDs updated during this transaction. */
        private TreeSet<Integer> caches = new TreeSet<>();

        /**
         * @param nodes Primary nodes count from TX record.
         * @param totalNodes Primary + backup nodes count from TX record.
         */
        TxOccurrence(int nodes, int totalNodes) {
            this.nodes = nodes;
            this.totalNodes = totalNodes;
        }

        /**
         * Handles data entry from data record.
         * @param entry object updated.
         */
        void onDataEntry(DataEntry entry) {
            entriesUpdated++;

            caches.add(entry.cacheId());
        }
    }

    /**
     * @param <K> key type parameter.
     */
    private static class LruMap<K> {
        /** Max size of map after which eviction may start. */
        private int maxSize;

        /**
         * Evict prohibited boundary. If this number of usages is accumulate in eldest entry it will not be removed
         * anyway.
         */
        private int evictProhibited;

        /**
         * Evicted count. Number of entries removed during statistic accumulation. Zero value means all records were
         * processed, created top (popular combination search) is totally correct. Non zero means top may be not
         * correct.
         */
        private int evicted;

        /** Map with data. */
        private Map<K, Integer> map = new LinkedHashMap<K, Integer>(16, 0.75F, false) {
            @Override protected boolean removeEldestEntry(Map.Entry<K, Integer> eldest) {
                if (size() < maxSize)
                    return false;

                final boolean evictNow = eldest.getValue() < evictProhibited;

                if (evictNow)
                    evicted++;

                return evictNow;
            }
        };

        /**
         * @param maxSize Max size of map.
         * @param evictProhibited usages count, when evict became prohibited.
         */
        LruMap(int maxSize, int evictProhibited) {
            this.maxSize = maxSize;
            this.evictProhibited = evictProhibited;
        }
    }
}
