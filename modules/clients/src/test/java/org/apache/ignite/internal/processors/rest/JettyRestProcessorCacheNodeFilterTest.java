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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * Test REST with cache node filter.
 */
public class JettyRestProcessorCacheNodeFilterTest extends JettyRestProcessorCommonSelfTest {
    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setConsistentId(igniteInstanceName).setCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        IgniteCache<String, String> cache = createCacheWithNodeFilter();

        // Send request to cache to node 0.
        checkPut(cache, "k0", "v0");

        // Send request to cache to node 0 with redirection to node 2.
        checkPut(cache, "k0", "v0", "destId", grid(2).cluster().localNode().id().toString());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSize() throws Exception {
        IgniteCache<String, String> cache = createCacheWithNodeFilter();

        cache.put("k0", "v0");

        // Send request to cache to node 0.
        checkSize(cache);

        // Send request to cache to node 0 with redirection to node 2.
        checkSize(cache, "destId", grid(2).cluster().localNode().id().toString());
    }

    /** */
    private IgniteCache<String, String> createCacheWithNodeFilter() throws InterruptedException {
        // Start cache only on node 1.
        IgnitePredicate<ClusterNode> nodeFilter = n -> n.consistentId().toString().endsWith("1");

        IgniteCache<String, String> cache = grid(1).createCache(
            new CacheConfiguration<String, String>(DEFAULT_CACHE_NAME).setNodeFilter(nodeFilter));

        awaitPartitionMapExchange();

        return cache;
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @param pars Additional parameters.
     */
    private void checkPut(IgniteCache<String, String> cache, String key, String val, String... pars) throws Exception {
        String ret = content(cache.getName(), GridRestCommand.CACHE_PUT, F.concat(pars, "key", key, "val", val));

        info("Put command result: " + ret);

        assertEquals(STATUS_SUCCESS, responseField(ret, "successStatus"));

        assertEquals(val, cache.get(key));
    }

    /**
     * @param cache Cache.
     * @param pars Additional parameters.
     */
    private void checkSize(IgniteCache<String, String> cache, String... pars) throws Exception {
        String ret = content(cache.getName(), GridRestCommand.CACHE_SIZE, pars);

        info("Size command result: " + ret);

        assertEquals(STATUS_SUCCESS, responseField(ret, "successStatus"));

        assertEquals(cache.size(CachePeekMode.PRIMARY), responseField(ret, "response"));
    }

    /** */
    private int responseField(String res, String field) throws JsonProcessingException {
        return JSON_MAPPER.readTree(res).get(field).asInt();
    }
}
