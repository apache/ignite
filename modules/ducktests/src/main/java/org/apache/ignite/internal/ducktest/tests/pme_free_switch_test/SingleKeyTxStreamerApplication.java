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

package org.apache.ignite.internal.ducktest.tests.pme_free_switch_test;

import java.time.Duration;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class SingleKeyTxStreamerApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(jsonNode.get("cacheName").asText());

        int warmup = jsonNode.get("warmup").asInt();

        int key = 0;
        int cnt = 0;
        long initTime = 0;
        long maxLatency = -1;

        boolean record = false;

        while (!terminated()) {
            cnt++;

            long from = System.nanoTime();

            cache.put(key++ % 100, key); // Cycled update.

            long latency = System.nanoTime() - from;

            if (!record && cnt > warmup) {
                record = true;

                initTime = System.currentTimeMillis();

                markInitialized();
            }

            if (record) {
                if (maxLatency < latency)
                    maxLatency = latency;
            }

            if (cnt % 100 == 0)
                log.info("APPLICATION_STREAMED " + cnt + " transactions [max=" + maxLatency + "]");
        }

        recordResult("WORST_LATENCY", Duration.ofNanos(maxLatency).toMillis());
        recordResult("STREAMED", cnt - warmup);
        recordResult("MEASURE_DURATION", System.currentTimeMillis() - initTime);

        markFinished();
    }
}
