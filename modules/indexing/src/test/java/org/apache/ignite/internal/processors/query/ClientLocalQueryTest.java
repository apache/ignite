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

package org.apache.ignite.internal.processors.query;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for IGNITE-6807
 */
public class ClientLocalQueryTest extends GridCommonAbstractTest {
    /** Server node. */
    private Ignite server;

    /** Client node. */
    private Ignite client;

    /** Name of created cache */
    private static final String CACHE_NAME = "TestCache";

    /** Starts node with specified config. */
    Ignite startGridWithCfg(String name, IgniteConfiguration cfg) throws Exception {
        return startGrid(name, optimize(cfg), null);
    }

    /** Sets up grids */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrid("server");

        IgniteConfiguration clCfg = getConfiguration();
        clCfg.setClientMode(true);

        client = startGridWithCfg("client", clCfg);
    }

    /** Creates cache and test table for the test */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache cache = client.createCache(ccfg);

        cache.query(new SqlFieldsQuery("CREATE TABLE public.test_table (id LONG PRIMARY KEY, val LONG);"));
    }

    /** Destroy the cache. */
    @Override protected void afterTest() throws Exception {
        client.destroyCache(CACHE_NAME);

        super.afterTest();
    }

    /** Stops all grids. */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * If we are executing local query on client node, we will get reasonable exception message.
     * @throws Exception on error.
     */
    public void testLocalQueryIsUnsupportedOnClient() throws Exception {
        IgniteCache<Long, UUID> cache = client.cache(CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT count(id) FROM public.test_table;", true).setLocal(true);

        cache.query(qry).getAll();
        //TODO: assert exception message.
    }
}
