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

import java.util.TreeMap;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import static org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplicationStreamer.MAX_STREAMER_DATA_SIZE;

/**
 * Application generates cache data by specified parameters.
 */
public class DataGenerationApplicationBatchPutAll extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int backups = jsonNode.get("backups").asInt();
        int cacheCnt = jsonNode.get("cacheCount").asInt();
        int entrySize = jsonNode.get("entrySize").asInt();
        int from = jsonNode.get("from").asInt();
        int to = jsonNode.get("to").asInt();

        markInitialized();

        for (int i = 1; i <= cacheCnt; i++) {
            IgniteCache<Integer, DataModel> cache = ignite.getOrCreateCache(
                new CacheConfiguration<Integer, DataModel>("test-cache-" + i)
                    .setBackups(backups));

            generateCacheData(cache, entrySize, from, to);
        }

        markFinished();
    }

    /**
     * @param entrySize Entry size.
     * @param from From key.
     * @param to To key.
     */
    private void generateCacheData(IgniteCache<Integer, DataModel> cache, int entrySize, int from, int to) {
        int batchSize = MAX_STREAMER_DATA_SIZE / entrySize + (MAX_STREAMER_DATA_SIZE % entrySize == 0 ? 0 : 1);

        TreeMap<Integer, DataModel> map;

        int i = from;

        while(i < to) {
            map = new TreeMap<>();

            for (int j = 0; j < batchSize & i < to; j += entrySize, i++)
                map.put(i, new DataModel(entrySize));

            cache.putAll(map);
        }

        log.info(cache.getName() + " data generated [entryCnt=" + (from - to) + ", from=" + from + ", to=" + to + "]");
    }
}
