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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheSyncRebalanceModeSelfTest extends GridCommonAbstractTest {
    /** Entry count. */
    public static final int CNT = 100_000;

    /** */
    private static final String STATIC_CACHE_NAME = "static";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(STATIC_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStaticCache() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Object, Object> cache = ignite.cache(STATIC_CACHE_NAME);

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(STATIC_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100_000; i++)
                streamer.addData(i, i);
        }

        assertEquals(CNT, cache.localSize());

        Ignite ignite2 = startGrid(1);

        assertEquals(CNT, ignite2.cache(STATIC_CACHE_NAME).localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP));

        for (int i = 0; i < CNT; i++)
            assertEquals(i, ignite2.cache(STATIC_CACHE_NAME).localPeek(i));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDynamicCache() throws Exception {
        IgniteEx ignite = startGrid(0);

        String cacheName = "dynamic";

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100_000; i++)
                streamer.addData(i, i);
        }

        assertEquals(CNT, cache.localSize());

        Ignite ignite2 = startGrid(1);

        assertEquals(CNT, ignite2.cache(cacheName).localSize(CachePeekMode.PRIMARY, CachePeekMode.BACKUP));

        for (int i = 0; i < CNT; i++)
            assertEquals(i, ignite2.cache(cacheName).localPeek(i));
    }
}
