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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests that dynamically started caches with near configurations actually start with near caches on all nodes:
 * affinity, non-affinity and clients.
 */
@RunWith(Parameterized.class)
public class GridCacheNearDynamicStartTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_CNT = 3;

    /** */
    private static final String CLIENT_ID = "client";

    /** */
    private static final int NUM_ENTRIES = 1000;

    /** */
    @Parameterized.Parameters(name = "nodeCacheStart = {0}, nodeNearCheck = {1}")
    public static Iterable<Object[]> testParameters() {
        List<Object[]> params = new ArrayList<>();

        for (NODE_TYPE nodeStart: NODE_TYPE.values()) {
            for (NODE_TYPE nodeNearCheck: NODE_TYPE.values())
                params.add(new Object[]{ nodeStart, nodeNearCheck});
        }

        return params;
    }

    /** */
    @Parameterized.Parameter(0)
    public NODE_TYPE nodeStart;

    /** */
    @Parameterized.Parameter(1)
    public NODE_TYPE nodeCheck;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(SRV_CNT);
        startClientGrid(CLIENT_ID);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteEx ign = grid(0);

        ign.cacheNames().forEach(ign::destroyCache);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setConsistentId(igniteInstanceName);
    }

    /** */
    @Test
    public void test() throws Exception {
        startCache();

        IgniteEx ign = testNode(nodeCheck);

        IgniteCache<Integer, Integer> cache = ign.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < NUM_ENTRIES; ++i) {
            assertEquals((Integer)i, cache.get(i));

            if (ign.affinity(DEFAULT_CACHE_NAME).isPrimary(ign.localNode(), i))
                return;

            assertEquals((Integer)i, cache.localPeek(i, CachePeekMode.NEAR));
        }
    }

    /** */
    private void startCache() {
        Ignite ign = testNode(nodeStart);

        ign.createCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setNodeFilter(n -> {
                    if (n.consistentId() == null)
                        return false;

                    // Start cache on nodes with indices [0, 1].
                    return !n.consistentId().toString().contains(String.valueOf(SRV_CNT - 1));
                })
                .setNearConfiguration(new NearCacheConfiguration<>())
        );

        try (IgniteDataStreamer<Integer, Integer> streamer = ign.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < NUM_ENTRIES; ++i)
                streamer.addData(i, i);

            streamer.flush();
        }

        assertEquals(ign.cache(DEFAULT_CACHE_NAME).size(CachePeekMode.PRIMARY), 1000);
    }

    /** */
    private IgniteEx testNode(NODE_TYPE type) {
        switch (type) {
            case AFFINITY:
                return grid(SRV_CNT - 2);
            case NON_AFFINITY:
                return grid(SRV_CNT - 1);
            case CLIENT:
            default:
                return grid(CLIENT_ID);
        }
    }

    /** */
    private enum NODE_TYPE {
        /** Affinity node. */
        AFFINITY,

        /** Non affinity node. */
        NON_AFFINITY,

        /** Client node. */
        CLIENT,
    }
}
