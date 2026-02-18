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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class ReplicatedNearCacheTest extends GridCommonAbstractTest {

    /** */
    private static final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(NODES - 1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheCreatedOnClient() throws Exception {
        NearCacheConfiguration<Integer, Integer> nearCfg = new NearCacheConfiguration<>();

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setNearConfiguration(nearCfg);

        IgniteEx srv = ignite(0);
        assertFalse(srv.configuration().isClientMode());

        Ignite cli = grid(NODES - 1);
        assertTrue(cli.configuration().isClientMode());

        IgniteCache<Integer, Integer> srvCache = srv.createCache(ccfg);
        assertFalse(((GatewayProtectedCacheProxy)srvCache).context().isNear());

        IgniteCache<Object, Object> cliCache = cli.cache(DEFAULT_CACHE_NAME);
        assertFalse(((IgniteCacheProxy)cliCache).context().isNear());

        // Fails here
        IgniteCache<Integer, Integer> cliNearCache = cli.getOrCreateNearCache(DEFAULT_CACHE_NAME, nearCfg);
        assertTrue(((IgniteCacheProxy)cliNearCache).context().isNear());
    }
}
