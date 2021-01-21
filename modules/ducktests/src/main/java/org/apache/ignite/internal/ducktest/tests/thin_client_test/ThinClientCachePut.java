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
        int entry_num = jsonNode.get("entry_num").asInt();

        markInitialized();

        System.out.println();
        System.out.println(">>> Cache test started.");

        ClientCacheConfiguration cfg = new ClientCacheConfiguration();
        cfg.setName(cacheName);

        ClientCache<Long, Long> cache = client.getOrCreateCache(cfg);

        System.out.println(">>> Cache created [cacheName=" + cache.getName() + ']');

        fillCache(cache, entry_num);

        checkCacheData(cache, entry_num);

        // Delete cache with its content completely.
        client.destroyCache(cacheName);

        markFinished();
    }
//    protected void run(JsonNode jsonNode) throws Exception {
//        String cacheName = jsonNode.get("cache_name").asText();
//        int entry_num = jsonNode.get("entry_num").asInt();
//
//        markInitialized();
//
//        System.out.println();
//        System.out.println(">>> Cache test started.");
//
//        ClientCache<Long, Long> cache = client.getOrCreateCache(cacheName);
//
//        System.out.println(">>> Cache created [cacheName=" + cache.getName() + ']');
//
//        fillCache(cache, entry_num);
//
//        checkCacheData(cache, entry_num);
//
//        // Delete cache with its content completely.
//        client.destroyCache(cacheName);
//
//        markFinished();
//    }
    /**
     * Fills cache.
     */
    public static void fillCache(ClientCache<Long, Long> cache, int entry_num) {
        System.out.println();
        System.out.println(">>> Fill cache with values [cacheName=" + cache.getName() + ']');

        for (long i = 0; i < entry_num; i++) {
            cache.put(i, i);

            System.out.println("Put [key=" + i + ", val=" + i + ']');
        }
    }

    /**
     * Checks cache data.
     */
    public static void checkCacheData(ClientCache<Long, Long> cache, int entry_num) {
        System.out.println();
        System.out.println(">>> Check cache data [cacheName=" + cache.getName() + ']');
        System.out.println(">>> Check cache data [entry_num=" + entry_num + ']');
        System.out.println(">>> Check cache data [cache.size=" + cache.size() + ']');

        assert (entry_num == cache.size());

        for (long i = 0; i < entry_num; i++) {
            Long val = cache.get(i);

            System.out.println("Got [key=" + i + ", val=" + val + ']');

            assert (i == val);
        }
    }
}
