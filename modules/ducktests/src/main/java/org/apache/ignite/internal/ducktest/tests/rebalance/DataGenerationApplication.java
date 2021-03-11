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

package org.apache.ignite.internal.ducktest.tests.rebalance;

import java.util.Random;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** Max streamer data size. */
    private static final long MAX_STREAMER_DATA_SIZE = 100_000_000;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int backups = jsonNode.get("backups").asInt();
        int cacheCnt = jsonNode.get("cacheCount").asInt();
        int entryCnt = jsonNode.get("entryCount").asInt();
        int entrySize = jsonNode.get("entrySize").asInt();

        log.info("Data generation started [backups=" + backups + ", cacheCount=" + cacheCnt
            + ", entryCount=" + entryCnt + ", entrySize=" + entrySize + "]");

        for (int i = 1; i <= cacheCnt; i++) {
            IgniteCache<Integer, DataModel> cache = createCache("test-cache-" + i, backups);

            generateCacheData(cache.getName(), entryCnt, entrySize);
        }

        log.info("Data generation finished.");

        markSyncExecutionComplete();
    }

    /**
     * @param cacheName Cache name.
     * @param entryCnt Entry count.
     * @param entrySize Entry size.
     */
    private void generateCacheData(String cacheName, int entryCnt, int entrySize) {
        int logStreamedEntriesQuant = (int)Math.pow(10, (int)Math.log10(entryCnt) - 1);

        try (IgniteDataStreamer<Integer, DataModel> stmr = ignite.dataStreamer(cacheName)) {
            for (int i = 0, n = 0; i < entryCnt; i++) {
                stmr.addData(i, new DataModel(entrySize));

                int streamed = i + 1;

                if (streamed % logStreamedEntriesQuant == 0)
                    log.info("Streamed " + streamed + " entries into " + cacheName);

                n += entrySize;

                if (n >= MAX_STREAMER_DATA_SIZE) {
                    n = 0;

                    stmr.flush();
                }
            }
        }

        if (entryCnt % logStreamedEntriesQuant != 0)
            log.info("Streamed " + entryCnt + " entries into " + cacheName);

        log.info(cacheName + " data generated.");
    }

    /**
     * @param name Cache name.
     * @param backups Backups.
     */
    private <K, V> IgniteCache<K, V> createCache(String name, int backups) {
        return ignite.createCache(new CacheConfiguration<K, V>(name)
            .setBackups(backups));
    }

    /**
     *
     */
    private static class DataModel {
        /** Cached payload. */
        private static byte[] cachedPayload;

        /** Payload. */
        final byte[] payload;

        /**
         * @param entrySize Entry size.
         */
        DataModel(int entrySize) {
            payload = getPayload(entrySize);
        }

        /**
         * @param payloadSize Payload size.
         */
        private static byte[] getPayload(int payloadSize) {
            if (cachedPayload == null || cachedPayload.length != payloadSize) {
                cachedPayload = new byte[payloadSize];

                new Random(42).nextBytes(cachedPayload);
            }

            return cachedPayload;
        }
    }
}
