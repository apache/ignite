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

package org.apache.ignite.internal.ducktest.tests.cdc;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Application continuously writes to cache spreading keys among clusters.
 */
public class CdcContinuousUpdatesApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        String cacheName = jsonNode.get("cacheName").asText();

        int range = jsonNode.get("range").asInt();

        int clusterIdx = jsonNode.get("clusterIdx").asInt();

        int clusterCnt = Optional.ofNullable(jsonNode.get("clusterCnt"))
            .map(JsonNode::asInt)
            .orElse(2);

        long pacing = Optional.ofNullable(jsonNode.get("pacing"))
            .map(JsonNode::asLong)
            .orElse(0L);

        assert clusterIdx < clusterCnt;

        log.info("Test props:" +
            " cacheName=" + cacheName +
            " range=" + range +
            " clusterCnt=" + clusterCnt +
            " clusterIdx=" + clusterIdx);

        IgniteCache<Integer, UUID> cache = ignite.getOrCreateCache(cacheName);
        log.info("Node name: " + ignite.name() + " starting cache operations.");

        markInitialized();

        long putCnt = 0;

        while (!terminated()) {
            int key = ThreadLocalRandom.current().nextInt(range);

            if (key % clusterCnt != clusterIdx)
                continue;

            UUID uuid = UUID.randomUUID();

            cache.put(key, uuid);

            putCnt++;

            Thread.sleep(pacing);
        }

        long removeCnt = 0;

        for (int i = 0; i < range / 4; i++) {
            int key = ThreadLocalRandom.current().nextInt(range);

            if (key % clusterCnt != clusterIdx)
                continue;

            cache.remove(key);

            removeCnt++;
        }

        log.info("Finished cache operations [node=" + ignite.name() + ", putCnt=" + putCnt + ", removeCnt=" + removeCnt + "]");

        recordResult("putCnt", putCnt);
        recordResult("removeCnt", removeCnt);

        markFinished();
    }
}
