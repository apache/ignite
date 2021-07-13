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

package org.apache.ignite.internal.ducktest.tests.client_test;

import java.util.Optional;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Java client. Tx put operation
 */
public class IgniteCachePutClient extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        String cacheName = jsonNode.get("cacheName").asText();

        long pacing = Optional.ofNullable(jsonNode.get("pacing"))
                .map(JsonNode::asLong)
                .orElse(0L);

        log.info("Test props:" +
                " cacheName=" + cacheName +
                " pacing=" + pacing);

        IgniteCache<UUID, UUID> cache = ignite.getOrCreateCache(cacheName);
        log.info("Node name: " + ignite.name() + " starting cache operations.");

        markInitialized();

        while (!terminated()) {
            UUID uuid = UUID.randomUUID();

            long startTime = System.nanoTime();

            cache.put(uuid, uuid);

            long resultTime = System.nanoTime() - startTime;

            log.info("Success put, latency: " + resultTime + "ns.");

            Thread.sleep(pacing);
        }

        markFinished();
    }
}
