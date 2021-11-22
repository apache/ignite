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

package org.apache.ignite.internal.ducktest.tests.cellular_affinity_test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;

/**
 *  Prepares transactions at specified cell.
 */
public class CellularPreparedTxStreamer extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        final String cacheName = jsonNode.get("cacheName").asText();
        final String attr = jsonNode.get("attr").asText();
        final String cell = jsonNode.get("cell").asText();
        final int txCnt = jsonNode.get("colocatedTxCnt").asInt();
        final int multiTxCnt = jsonNode.get("multiTxCnt").asInt();
        final int noncolocatedTxCnt = jsonNode.get("noncolocatedTxCnt").asInt();

        final String avoidCell = "C0"; // Always exist, should show speed of non-affected cell.

        markInitialized();

        waitForActivation();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

        log.info("Starting Prepared Txs...");

        Affinity<Integer> aff = ignite.affinity(cacheName);

        int cnt = 0;
        int i = 0; // Negative keys to have no intersection with load.

        // Single key transactions affects only current cell.
        // Will cause delay during current cell switch.

        while (cnt != txCnt && !terminated()) {
            if (getCellIdByKey(aff, --i, attr).equals(cell)) {
                Transaction tx = ignite.transactions().txStart();

                cache.put(i, i);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                if (cnt++ % 100 == 0)
                    log.info("Long Tx prepared [key=" + i + ",cnt=" + cnt + "]");
            }
        }

        // Multikey transactions.
        // May cause delay during current and other cell switch.

        cnt = 0;

        assert i > -10_000_000;
        i = -10_000_000; // To have no intersection with other node's txs.

        while (cnt != multiTxCnt && !terminated()) {
            Set<Integer> keys = new HashSet<>();

            while (keys.size() < 3) {
                if (!getCellIdByKey(aff, --i, attr).equals(avoidCell))
                    keys.add(i);
            }

            Transaction tx = ignite.transactions().txStart();

            for (int key : keys)
                cache.put(key, key);

            ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

            if (cnt++ % 100 == 0)
                log.info("Long Multikey Tx prepared [key=" + i + ",cnt=" + cnt + "]");
        }

        // Transactions started from this node but contain no local keys.
        // Should not cause significant delay during any cell switch.

        cnt = 0;
        assert i > -20_000_000;
        i = -20_000_000; // To have no intersection with other node's txs.

        while (cnt != noncolocatedTxCnt && !terminated()) {
            String keyCell = getCellIdByKey(aff, --i, attr);

            if (!keyCell.equals(cell) && !keyCell.equals(avoidCell)) {
                Transaction tx = ignite.transactions().txStart();

                cache.put(i, i);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                if (cnt++ % 100 == 0)
                    log.info("Long Noncolocated Tx prepared [key=" + i + ",cnt=" + cnt + "]");
            }
        }

        log.info("ALL_TRANSACTIONS_PREPARED (" + cnt + ")");

        while (!terminated()) {
            log.info("Waiting for SIGTERM.");

            U.sleep(1000);
        }

        markFinished();
    }

    /**
     *
     */
    private String getCellIdByKey(Affinity<Integer> aff, int key, String attr) {
        Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        Map<Object, Long> stat = nodes.stream().collect(
            Collectors.groupingBy(n -> n.attributes().get(attr), Collectors.counting()));

        assert 1 == stat.keySet().size() :
            "Partition should be located on nodes from only one cell " +
                "[key=" + key + ", nodes=" + nodes.size() + ", stat=" + stat + "]";

        return (String)stat.keySet().iterator().next();
    }
}
