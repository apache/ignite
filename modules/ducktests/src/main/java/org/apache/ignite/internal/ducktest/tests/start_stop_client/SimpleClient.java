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

package org.apache.ignite.internal.ducktest.tests.start_stop_client;

import java.util.Optional;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Java client. Tx put operation
 */

public class SimpleClient extends IgniteAwareApplication {

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        String cacheName = Optional.ofNullable(jsonNode.get("cacheName")).map(JsonNode::asText).orElse("default-cache-name");
        long pacing = Optional.ofNullable(jsonNode.get("pacing")).map(JsonNode::asLong).orElse(0l);

        log.info("test props:" +
                " cacheName=" + cacheName +
                " pacing=" + pacing);

        IgniteCache<UUID, UUID> cache = ignite.getOrCreateCache(prepareCacheConfiguration(cacheName));
        log.info("node name: " + ignite.name() + " starting cache operations");

        markInitialized();
        while (!terminated()) {
            cacheOperation(cache);
            Thread.sleep(pacing);
        }
        markFinished();
    }

    /** cache config
     * @param cacheName - name of target cache*/
    private CacheConfiguration prepareCacheConfiguration(String cacheName) {
        CacheConfiguration<?,?> cfg = new CacheConfiguration();
        cfg.setBackups(2);
        cfg.setName(cacheName);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        return cfg;
    }

    /** single cache operation.
     * @param cache - target cache*/
    private void cacheOperation(IgniteCache<UUID,UUID> cache) throws InterruptedException {
        UUID key = UUID.randomUUID();
        long startTime = System.nanoTime();
        cache.put(key,key);
        long resultTime = System.nanoTime() - startTime;
        log.info("success put key=" + key + " value=" + key + " latency: " + resultTime + "ns");
    }
}
