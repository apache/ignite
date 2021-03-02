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
        IgniteCache<Integer, DataModel> cache = ignite.createCache(
            new CacheConfiguration<Integer, DataModel>("test-cache-" + cacheNo)
                .setBackups(1));

        try (IgniteDataStreamer<Integer, DataModel> stmr = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < entryCnt; i++) {
                stmr.addData(i, newDataModel(entrySize));

                if (i % 10_000 == 0)
                    log.info("Streamed " + i + " entries");
            }
        }
    }

    /**
     * @param payloadSize Payload size.
     */
    private DataModel newDataModel(int payloadSize) {
        String randomStr = UUID.randomUUID().toString();

        byte[] payload = new byte[payloadSize - randomStr.length()];

        ThreadLocalRandom.current().nextBytes(payload);

        return new DataModel(randomStr, payload);
    }

    /**
     *
     */
    private static class DataModel {
        /** Random string. */
        final String randomStr;

        /** Payload. */
        final byte[] payload;

        /**
         * @param randomStr Random string.
         * @param payload Payload.
         */
        DataModel(String randomStr, byte[] payload) {
            this.randomStr = randomStr;
            this.payload = payload;
        }
    }
}
