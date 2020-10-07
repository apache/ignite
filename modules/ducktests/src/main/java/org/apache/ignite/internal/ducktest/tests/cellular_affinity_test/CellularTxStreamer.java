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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
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

        long[] max = new long[20];

        Arrays.fill(max, -1);

        int key = 0;

        int cnt = 0;

        long initTime = 0;

        boolean record = false;

        Affinity<Integer> aff = ignite.affinity(cacheName);

        while (!terminated()) {
            key++;

            Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

            Map<Object, Long> stat = nodes.stream().collect(
                Collectors.groupingBy(n -> n.attributes().get(attr), Collectors.counting()));

            if (!stat.containsKey(cell))
                continue;

            cnt++;

            long start = System.currentTimeMillis();

            cache.put(key, key);

            long finish = System.currentTimeMillis();

            long time = finish - start;

            if (!record && cnt > warmup) {
                record = true;

                initTime = System.currentTimeMillis();

                log.info("WARMUP_FINISHED");
            }

            if (record) {
                for (int i = 0; i < max.length; i++) {
                    if (max[i] <= time) {
                        System.arraycopy(max, i, max, i + 1, max.length - i - 1);

                        max[i] = time;

                        break;
                    }
                }
            }

            if (cnt % 1000 == 0)
                log.info("APPICATION_STREAMED " + cnt + " transactions [worst_latency=" + Arrays.toString(max) + "]");
        }

        recordResult("WORST_LATENCY", Arrays.toString(max));
        recordResult("STREAMED", cnt - warmup);
        recordResult("MEASURE_DURATION", System.currentTimeMillis() - initTime);

        markFinished();
    }
}
