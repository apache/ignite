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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Streams transactions to specified cell.
 */
public class CellularTxStreamer extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws Exception {
        String cacheName = jsonNode.get("cacheName").asText();
        int warmup = jsonNode.get("warmup").asInt();
        String cell = jsonNode.get("cell").asText();
        String attr = jsonNode.get("attr").asText();

        markInitialized();

        waitForActivation();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

        int precision = 5;

        long[] latencies = new long[precision];
        LocalDateTime[] opStartTimes = new LocalDateTime[precision];

        Arrays.fill(latencies, -1);

        int cnt = 0;

        long initTime = 0;

        boolean record = false;

        Affinity<Integer> aff = ignite.affinity(cacheName);

        List<Integer> cellKeys = new ArrayList<>();

        int candidate = 0;

        while (cellKeys.size() < 100) {
            Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(++candidate);

            Set<ClusterNode> stat = nodes.stream()
                .filter(n -> n.attributes().get(attr).equals(cell))
                .collect(Collectors.toSet());

            if (stat.isEmpty())
                continue;

            assert nodes.size() == stat.size();

            cellKeys.add(candidate);
        }

        while (!terminated()) {
            cnt++;

            LocalDateTime start = LocalDateTime.now();

            long from = System.nanoTime();

            cache.put(cellKeys.get(cnt % cellKeys.size()), cnt); // Cycled update.

            long latency = System.nanoTime() - from;

            if (!record && cnt > warmup) {
                record = true;

                initTime = System.currentTimeMillis();

                log.info("WARMUP_FINISHED");
            }

            if (record) {
                for (int i = 0; i < latencies.length; i++) {
                    if (latencies[i] <= latency) {
                        System.arraycopy(latencies, i, latencies, i + 1, latencies.length - i - 1);
                        System.arraycopy(opStartTimes, i, opStartTimes, i + 1, opStartTimes.length - i - 1);

                        latencies[i] = latency;
                        opStartTimes[i] = start;

                        break;
                    }
                }
            }

            if (cnt % 1000 == 0)
                log.info("APPLICATION_STREAMED " + cnt + " transactions [worst_latency=" + Arrays.toString(latencies) + "]");
        }

        List<String> result = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

        for (int i = 0; i < precision; i++)
            result.add(Duration.ofNanos(latencies[i]).toMillis() + " ms at " + formatter.format(opStartTimes[i]));

        recordResult("WORST_LATENCY", result.toString());
        recordResult("STREAMED", cnt - warmup);
        recordResult("MEASURE_DURATION", System.currentTimeMillis() - initTime);

        markFinished();
    }
}
