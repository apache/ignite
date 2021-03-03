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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int cacheCnt = jsonNode.get("cacheCount").asInt();
        int entryCnt = jsonNode.get("entryCount").asInt();
        int entrySize = jsonNode.get("entrySize").asInt();

        log.info("Creating " + cacheCnt + " caches each with " + entryCnt + " entries of " + entrySize + " bytes.");

        for (int i = 1; i <= cacheCnt; i++)
            generateCache(i, entryCnt, entrySize);

        markSyncExecutionComplete();
    }

    /**
     * @param cacheNo Cache number.
     * @param entryCnt Entry count.
     * @param entrySize Entry size.
     */
    private void generateCache(int cacheNo, int entryCnt, int entrySize) {
        IgniteCache<Integer, DataModel> cache = createCache("test-cache-" + cacheNo, 1);

        try (IgniteDataStreamer<Integer, DataModel> stmr = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < entryCnt; i++) {
                stmr.addData(i, new DataModel(entrySize));

                int streamed = i + 1;

                if (streamed % 5_000 == 0)
                    log.info("Streamed " + streamed + " entries into " + cache.getName());
            }

            if (entryCnt % 5_000 != 0)
                log.info("Streamed " + entryCnt + " entries into " + cache.getName());
       }
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
        /** Random string samples. */
        private static final String[] randomStringSamples = new String[1000];

        static {
            for (int i = 0; i < randomStringSamples.length; i++)
                randomStringSamples[i] = UUID.randomUUID().toString();
        }

        /** Cached payload. */
        private static byte[] cachedPayload;

        /** Random string. */
        final String randomStr;

        /** Payload. */
        final byte[] payload;

        /**
         * @param entrySize Entry size.
         */
        DataModel(int entrySize) {
            randomStr = randomStringSamples[ThreadLocalRandom.current().nextInt(randomStringSamples.length)];
            payload = getPayload(entrySize - randomStr.length());
        }

        /**
         * @param payloadSize Payload size.
         */
        private static byte[] getPayload(int payloadSize) {
            if (cachedPayload == null || cachedPayload.length != payloadSize) {
                cachedPayload = new byte[payloadSize];

                ThreadLocalRandom.current().nextBytes(cachedPayload);
            }

            return cachedPayload;
        }
    }
}
