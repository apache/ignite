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
import java.util.Map;
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
 *
 */
public class PreparedTxStreamer extends IgniteAwareApplication {
    /**
     * {@inheritDoc}
     */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        final String cacheName = jsonNode.get("cacheName").asText();
        final String attr = jsonNode.get("attr").asText();
        final String cell = jsonNode.get("cell").asText();
        final int txCnt = jsonNode.get("txCnt").asInt();

        markInitialized();

        waitForActivation();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

        log.info("Starting Prepared Txs...");

        Affinity<Integer> aff = ignite.affinity(cacheName);

        int cnt = 0;
        int i = -1; // Negative keys to have no intersection with load.

        while (cnt != txCnt && !terminated()) {
            Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(i);

            Map<Object, Long> stat = nodes.stream().collect(
                Collectors.groupingBy(n -> n.attributes().get(attr), Collectors.counting()));

            assert 1 == stat.keySet().size() :
                "Partition should be located on nodes from only one cell " +
                    "[key=" + i + ", nodes=" + nodes.size() + ", stat=" + stat + "]";

            if (stat.containsKey(cell)) {
                cnt++;

                Transaction tx = ignite.transactions().txStart();

                cache.put(i, i);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                if (cnt % 100 == 0)
                    log.info("Long Tx prepared [key=" + i + ",cnt=" + cnt + ", cell=" + stat.keySet() + "]");
            }

            i--;
        }

        log.info("All transactions prepared (" + cnt + ")");

        while (!terminated()) {
            log.info("Waiting for SIGTERM.");

            U.sleep(1000);
        }

        markFinished();
    }
}
