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

package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class TxWalState {
    /** Preparing txs. */
    private final Map<GridCacheVersion, TxHolder> preparing = new HashMap<>();

    /** Prepared txs. */
    private final Map<GridCacheVersion, TxHolder> prepared = new HashMap<>();

    /** Commited txs. */
    private final Set<GridCacheVersion> commited = new HashSet<>();

    /** RollBacked txs. */
    private final Set<GridCacheVersion> rollbacked = new HashSet<>();

    /**
     *
     */ //TODO resolve via baseline.
    private Set<String> nodes(Map<Object, Set<Object>> map) {
        Set<String> constIds = new HashSet<>();

        for (Map.Entry<Object, Set<Object>> entry : map.entrySet()) {
            Object key = entry.getKey();

            constIds.add(key.toString());

            for (Object o : entry.getValue())
                constIds.add(o.toString());
        }

        return constIds;
    }

    /**
     * On preparing tx record.
     *
     * @param tx Transaction record.
     */
    public void onPreparing(TxRecord tx) {
        GridCacheVersion txVer = tx.nearXidVersion();

        TxHolder txHolder = preparing.get(txVer);

        if (txHolder == null) {
            txHolder = prepared.get(txVer);

            if (txHolder == null)
                txHolder = TxHolder.create(txVer, tx.participatingNodes());

            preparing.put(txVer, txHolder);
        }

        txHolder.preparing++;
    }

    /**
     * On prepared tx record.
     *
     * @param tx Transaction record.
     */
    public void onPrepared(TxRecord tx) {
        GridCacheVersion txVer = tx.nearXidVersion();

        TxHolder txHolderPreparing = preparing.get(txVer);

        if (txHolderPreparing == null)
            return;

        if (--txHolderPreparing.preparing == 0)
            preparing.remove(txVer);

        TxHolder txHolderPrepared = this.prepared.get(txVer);

        if (txHolderPrepared == null) {
            txHolderPreparing.prepared = 1;

            txHolderPrepared = txHolderPreparing;

            this.prepared.put(txVer, txHolderPreparing);
        }
        else
            txHolderPrepared.prepared++;

        txHolderPrepared.merge(tx.participatingNodes());
    }

    /**
     * On commited tx record.
     *
     * @param tx Transaction record.
     */
    public void onCommited(TxRecord tx) {
        GridCacheVersion txVer = tx.nearXidVersion();

        TxHolder txHolder = prepared.get(txVer);

        if (txHolder == null)
            return;

        if (--txHolder.prepared == 0) {
            prepared.remove(txVer);

            commited.add(txVer);
        }
    }

    /**
     * On rollback tx record.
     *
     * @param tx Transaction record.
     */
    public void onRollbacked(TxRecord tx) {
        GridCacheVersion txVer = tx.nearXidVersion();

        TxHolder txHolder = prepared.get(txVer);

        if (txHolder == null)
            return;

        if (--txHolder.prepared == 0) {
            prepared.remove(txVer);

            rollbacked.add(txVer);
        }
    }

    /**
     * @param txVer Transaction id.
     * @return Boolean.
     */
    public boolean isPreparing(GridCacheVersion txVer) {
        return preparing.get(txVer) != null;
    }

    /**
     * @param txVer Transaction id.
     * @return Boolean.
     */
    public boolean isPrepared(GridCacheVersion txVer) {
        return prepared.get(txVer) != null;
    }

    /**
     * @param txVer Transaction id.
     * @return Boolean.
     */
    public boolean isCommited(GridCacheVersion txVer) {
        return commited.contains(txVer);
    }

    /**
     * @param txVer Transaction id.
     * @return Boolean.
     */
    public boolean isRollBacked(GridCacheVersion txVer) {
        return rollbacked.contains(txVer);
    }

    /**
     * @return Set of preparing transaction ids.
     */
    public Set<GridCacheVersion> preparingTxs() {
        return preparing.keySet();
    }

    /**
     * @return Map of prepared transaction ids.
     */
    public Map<GridCacheVersion, Set<String>> preparedTxs() {
        Map<GridCacheVersion, Set<String>> m = new HashMap<>();

        for (Map.Entry<GridCacheVersion, TxHolder> entry : prepared.entrySet()) {
            TxHolder txHolder = entry.getValue();

            if (txHolder.preparing == 0)
                m.put(txHolder.txVer, nodes(txHolder.nodes));
        }

        return m;
    }

    /**
     * @return Set of commited transaction ids.
     */
    public Set<GridCacheVersion> commitedTxs() {
        return commited;
    }

    /**
     * @return Set of rollBacked transaction ids.
     */
    public Set<GridCacheVersion> rollBackedTxs() {
        return rollbacked;
    }

    /**
     * Append debug info.
     *
     * @param rb Recovery debug.
     */
    public void appendDebugInfo(RecoveryFuture.RecoveryDebug rb) {
        rb.append("preparing ").append(preparing.size());

        rb.append("\n");

        if (!preparing.isEmpty()) {
            for (TxHolder txHolder : preparing.values())
                rb.append(txHolder).append("\n");
        }

        rb.append("prepared ").append(prepared.size());

        if (!prepared.isEmpty()) {
            rb.append("\n");

            for (TxHolder txHolder : prepared.values())
                rb.append(txHolder).append("\n");
        }

        rb.append("\n");

        rb.append("commited ").append(commited.size()).append("\n");
        rb.append("rollBacked ").append(rollbacked.size()).append("\n");
    }

    /**
     * Transaction wal record wrapper.
     */
    private static class TxHolder {
        /** Counter preparing markers. */
        private int preparing;

        /** Counter prepared markers. */
        private int prepared;

        /** Transaction id. */
        private final GridCacheVersion txVer;

        /** Nodes participated in transaction. */
        private final Map<Object, Set<Object>> nodes;

        /**
         *
         */
        private TxHolder(GridCacheVersion txVer, Map<Object, Set<Object>> nodes) {
            this.txVer = txVer;
            this.nodes = nodes;
        }

        /**
         * Factory method.
         */
        private static TxHolder create(GridCacheVersion txVer, Map<Object, Collection<Object>> nodes) {
            Map<Object, Set<Object>> map = new HashMap<>();

            for (Map.Entry<Object, Collection<Object>> entry : nodes.entrySet())
                map.put(entry.getKey(), new HashSet<>(entry.getValue()));

            return new TxHolder(txVer, map);
        }

        /**
         *
         */
        private void merge(Map<Object, Collection<Object>> nodes) {
            for (Map.Entry<Object, Collection<Object>> entry : nodes.entrySet()) {
                Object constId = entry.getKey();

                Collection<Object> backups = this.nodes.get(constId);

                if (backups == null)
                    this.nodes.put(constId, new HashSet<>(entry.getValue()));
                else
                    backups.addAll(entry.getValue());
            }
        }

        /**
         *
         */
        private String constIdsToString(Map<Object, Set<Object>> nodes) {
            StringBuilder sb = new StringBuilder();

            if (nodes.isEmpty())
                sb.append("[]");

            for (Map.Entry<Object, Set<Object>> entry : nodes.entrySet()) {
                sb.append("(").append(entry.getKey()).append("->");

                Collection<Object> backUps = entry.getValue();

                boolean empty = backUps.isEmpty();

                if (!empty)
                    sb.append("[");
                else {
                    sb.append("[]");

                    continue;
                }

                int size = backUps.size();

                int last = size - 1;

                int cnt = 0;

                for (Object node : entry.getValue()) {
                    sb.append(node);

                    if (cnt != last)
                        sb.append(",");

                    cnt++;
                }

                sb.append("]");

                sb.append(")");
            }

            return sb.toString();
        }

        private String txVer() {
            return "ver[topVer=" + txVer.topologyVersion() +
                ", order=" + txVer.order() +
                ", nodeOrder=" + txVer.nodeOrder() + ']';
        }

        @Override public String toString() {
            return "Tx[" +
                "preparing=" + preparing +
                " prepared=" + prepared +
                " " + txVer() +
                " nodes=" + constIdsToString(nodes) +
                ']';
        }
    }
}
