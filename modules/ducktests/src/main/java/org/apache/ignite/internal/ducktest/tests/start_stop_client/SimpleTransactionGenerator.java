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
import org.apache.ignite.internal.ducktest.tests.start_stop_client.node.ActionNode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Java client. Tx put operation.
 */

public class SimpleTransactionGenerator extends ActionNode {

    /** Target cache. */
    private IgniteCache cache;

    /** Logger. */
    private static final Logger log = LogManager.getLogger(SimpleTransactionGenerator.class.getName());

    /** Value for test. */
    private static final String VALUE = "Client start stop simple test.";

    /** {@inheritDoc} */
    @Override public long singleAction() {

        UUID key = UUID.randomUUID();
        long startTime = System.nanoTime();
        cache.put(key,VALUE);
        long resultTime = System.nanoTime() - startTime;

        return resultTime;
    }

    /** {@inheritDoc} */
    @Override protected void scriptInit(JsonNode jsonNode) {

        String cacheName = Optional.ofNullable(jsonNode.get("cacheName")).map(JsonNode::asText).orElse("default-cache-name");
        log.info("test props:" + " cacheName=" + cacheName );
        cache = ignite.getOrCreateCache(prepareCacheConfiguration(cacheName));
        log.info("node name: " + ignite.name() + " starting cache operations");

    }

    /**
     * Cache config.
     * @param cacheName - name of target cache.
     * */
    private CacheConfiguration prepareCacheConfiguration(String cacheName) {
        CacheConfiguration<?,?> cfg = new CacheConfiguration();
        cfg.setBackups(2);
        cfg.setName(cacheName);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        return cfg;
    }

}
