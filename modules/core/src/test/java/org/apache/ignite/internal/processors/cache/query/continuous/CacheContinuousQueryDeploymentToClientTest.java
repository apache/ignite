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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Map;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.AbstractContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for continuous query deployment to client nodes.
 */
public class CacheContinuousQueryDeploymentToClientTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test_cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test starts 1 server node and 1 client node. The client node deploys
     * CQ for the cache {@link #CACHE_NAME}. After that another client node is started.
     * Expected that CQ won't be deployed to the new client, since the client doesn't
     * store any data.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeploymentToNewClient() throws Exception {
        startGrid(0);

        IgniteEx client1 = startClientGrid(1);

        IgniteCache<Integer, String> cache = client1.cache(CACHE_NAME);

        AbstractContinuousQuery<Integer, String> qry = new ContinuousQuery<Integer, String>()
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteFilterFactory((Factory<CacheEntryEventFilter<Integer, String>>)() -> evt -> true);

        cache.query(qry);

        IgniteEx client2 = startClientGrid(2);

        GridContinuousProcessor proc = client2.context().continuous();

        assertEquals(0, ((Map<?, ?>)U.field(proc, "locInfos")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "rmtInfos")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "startFuts")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "stopFuts")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "bufCheckThreads")).size());
    }

    /**
     * Test starts 1 server node and 2 client nodes. The first client node deploys
     * CQ for the cache {@link #CACHE_NAME}.
     * Expected that CQ won't be deployed to the second client, since the client doesn't
     * store any data.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeploymentToExistingClient() throws Exception {
        startGrid(0);

        IgniteEx client1 = startClientGrid(1);

        IgniteCache<Integer, String> cache = client1.cache(CACHE_NAME);

        IgniteEx client2 = startClientGrid(2);

        AbstractContinuousQuery<Integer, String> qry = new ContinuousQuery<Integer, String>()
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteFilterFactory((Factory<CacheEntryEventFilter<Integer, String>>)() -> evt -> true);

        cache.query(qry);

        GridContinuousProcessor proc = client2.context().continuous();

        assertEquals(0, ((Map<?, ?>)U.field(proc, "locInfos")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "rmtInfos")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "startFuts")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "stopFuts")).size());
        assertEquals(0, ((Map<?, ?>)U.field(proc, "bufCheckThreads")).size());
    }
}
