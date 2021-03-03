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

package org.apache.ignite.internal.ducktest.tests.thin_client_test;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.ThinClientApplication;


/**
 * Thin client. Cache test: put, get, value check
 */
public class ThinClientCachePut extends ThinClientApplication {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void run(JsonNode jsonNode) throws Exception {
        String cacheName = jsonNode.get("cache_name").asText();
        String cacheMode = jsonNode.get("cache_mode").asText();
        String cacheAtomcityMode = jsonNode.get("cache_atomicity_mode").asText();
        int backups = jsonNode.get("backups").asInt();
        int entry_num = jsonNode.get("entry_num").asInt();

        markInitialized();

        ClientCacheConfiguration cfg = new ClientCacheConfiguration();
        cfg.setName(cacheName);

        if (cacheMode.equals(CacheMode.REPLICATED.toString())) { cfg.setCacheMode(CacheMode.REPLICATED);}
        else if (cacheMode.equals(CacheMode.PARTITIONED.toString())) {
            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setBackups(backups);
        }
        else {
            log.info(">>> CacheMode is not correct:" + cacheMode);
        }

        if (cacheAtomcityMode.equals(CacheAtomicityMode.ATOMIC.toString())) { cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);}
        else if (cacheAtomcityMode.equals(CacheAtomicityMode.TRANSACTIONAL.toString())) { cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);}
        else {
            log.info(">>> CacheAtomicityMode is not correct:" + cacheAtomcityMode);
        }

        ClientCache<Long, Long> cache = client.getOrCreateCache(cfg);

        assert (cacheMode.equals(cache.getConfiguration().getCacheMode().toString()));
        assert (cacheAtomcityMode.equals(cache.getConfiguration().getAtomicityMode().toString()));

        if (cacheMode.equals(CacheMode.PARTITIONED.toString())) {
            assert (backups == cache.getConfiguration().getBackups());
        }

        fillCache(cache, entry_num);

        checkCacheData(cache, entry_num);

        // Delete cache with its content completely.
        client.destroyCache(cacheName);

        markFinished();
    }

    /**
     * Fills cache.
     */
    public static void fillCache(ClientCache<Long, Long> cache, int entry_num) {
         for (long i = 0; i < entry_num; i++) {
            cache.put(i, i);
        }
    }

    /**
     * Checks cache data.
     */
    public static void checkCacheData(ClientCache<Long, Long> cache, int entry_num) {
        assert (entry_num == cache.size());

        for (long i = 0; i < entry_num; i++) {
            Long val = cache.get(i);
            assert (i == val);
        }
    }
}
