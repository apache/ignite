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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Information about found deadlock.
 */
public class TxDeadlock {
    /** Key prefix. */
    private static final String KEY_PREFIX = "K";

    /** Tx prefix. */
    private static final String TX_PREFIX = "TX";

    /** Tx locked keys. */
    private final Map<GridCacheVersion, Set<IgniteTxKey>> txLockedKeys;

    /** Tx requested keys. */
    private final Map<IgniteTxKey, Set<GridCacheVersion>> txRequestedKeys;

    /** Cycle. */
    private final List<GridCacheVersion> cycle;

    /** Transactions data: nearNodeId and threadId. */
    private final Map<GridCacheVersion, T2<UUID, Long>> txs;

    /**
     * @param cycle Cycle.
     * @param txs Transactions.
     * @param txLockedKeys Tx locked keys.
     * @param txRequestedKeys Tx requested keys.
     */
    public TxDeadlock(
        List<GridCacheVersion> cycle,
        Map<GridCacheVersion, T2<UUID, Long>> txs,
        Map<GridCacheVersion, Set<IgniteTxKey>> txLockedKeys,
        Map<IgniteTxKey, Set<GridCacheVersion>> txRequestedKeys
    ) {
        this.cycle = cycle;
        this.txLockedKeys = txLockedKeys;
        this.txRequestedKeys = txRequestedKeys;
        this.txs = txs;
    }

    /**
     * @return Deadlock represented as cycle of transaction in wait-for-graph.
     */
    public List<GridCacheVersion> cycle() {
        return cycle;
    }

    /**
     * @param ctx Context.
     */
    public String toString(GridCacheSharedContext ctx) {
        assert cycle != null && !cycle.isEmpty();

        assert cycle.size() >= 3; // At least 2 transactions in cycle and the last is waiting for the first.

        Map<IgniteTxKey, String> keyLabels = U.newLinkedHashMap(cycle.size() - 1);

        Map<GridCacheVersion, String> txLabels = U.newLinkedHashMap(cycle.size() - 1);

        StringBuilder sb = new StringBuilder("\nDeadlock detected:\n\n");

        for (int i = cycle.size() - 1; i > 0; i--) {
            GridCacheVersion txId = cycle.get(i);

            Set<IgniteTxKey> keys = txLockedKeys.get(txId);

            for (IgniteTxKey key : keys) {
                Set<GridCacheVersion> txIds = txRequestedKeys.get(key);

                if (txIds == null || txIds.isEmpty())
                    continue;

                GridCacheVersion waitsTx = null;

                for (GridCacheVersion ver : txIds) {
                    if (cycle.contains(ver)) {
                        waitsTx = ver;

                        break;
                    }
                }

                if (waitsTx != null) {
                    sb.append(label(key, KEY_PREFIX, keyLabels)).append(": ")
                        .append(label(txId, TX_PREFIX, txLabels)).append(" holds lock, ")
                        .append(label(waitsTx, TX_PREFIX, txLabels)).append(" waits lock.\n");
                }
            }
        }

        sb.append("\nTransactions:\n\n");

        for (Map.Entry<GridCacheVersion, String> e : txLabels.entrySet()) {
            T2<UUID, Long> tx = txs.get(e.getKey());

            sb.append(e.getValue()).append(" [txId=").append(e.getKey())
                .append(", nodeId=").append(tx.get1()).append(", threadId=").append(tx.get2())
                .append("]\n");
        }

        sb.append("\nKeys:\n\n");

        for (Map.Entry<IgniteTxKey, String> e : keyLabels.entrySet()) {
            IgniteTxKey txKey = e.getKey();

            try {
                GridCacheContext cctx = ctx.cacheContext(txKey.cacheId());

                Object val = txKey.key().value(cctx.cacheObjectContext(), true);

                sb.append(e.getValue())
                    .append(" [");
                if (S.includeSensitive())
                    sb.append("key=")
                        .append(val)
                        .append(", ");
                sb.append("cache=")
                    .append(cctx.name())
                    .append("]\n");
            }
            catch (Exception ex) {
                sb.append("Unable to unmarshall deadlock information for key [key=").append(e.getValue()).append("]\n");
            }
        }

        return sb.toString();
    }

    /**
     * @param id Id.
     * @param prefix Prefix.
     * @param map Map.
     */
    private static <T> String label(T id, String prefix, Map<T, String> map) {
        String lb = map.get(id);

        if (lb == null)
            map.put(id, lb = prefix + (map.size() + 1));

        return lb;
    }
}
