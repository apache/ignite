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

package org.apache.ignite.internal.processors.cache;

import java.util.Date;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for dynamic cache start.
 */
@SuppressWarnings("unchecked")
public class IgniteCacheStartStopLoadTest extends GridCommonAbstractTest {
    /** */
    private static final long DURATION = 60_000L;

    /** */
    private static final int CACHE_COUNT = 1;

    /** */
    private static final String[] CACHE_NAMES = new String[CACHE_COUNT];

    /** */
    static {
        for (int i = 0; i < CACHE_NAMES.length; i++)
            CACHE_NAMES[i] = "cache_" + i;
    }

    /** */
    private WeakHashMap<Object, Boolean> weakMap;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DURATION + 20_000L;
    }

    /**
     * @return Number of nodes for this test.
     */
    public int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(nodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMemoryLeaks() throws Exception {
        final Ignite ignite = ignite(0);

        long startTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startTime) < DURATION) {
            final AtomicInteger idx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    CacheConfiguration ccfg = new CacheConfiguration();

                    ccfg.setName(CACHE_NAMES[idx.getAndIncrement()]);

                    ignite.createCache(ccfg);

                    return null;
                }
            }, CACHE_COUNT, "cache-starter");

            for (String cacheName : CACHE_NAMES)
                assert ignite(0).cache(cacheName) != null;

            if (weakMap == null) {
                weakMap = new WeakHashMap<>();

                IgniteCache<Object, Object> cache = ignite(0).cache(CACHE_NAMES[0]);

                Object obj = new Date();

                cache.put(1, obj);

                weakMap.put(((IgniteCacheProxy)cache).delegate(), Boolean.TRUE);
                weakMap.put(obj, Boolean.TRUE);
            }

            idx.set(0);

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.destroyCache(CACHE_NAMES[idx.getAndIncrement()]);

                    return null;
                }
            }, CACHE_COUNT, "cache-starter");

            System.out.println("Start-Stop Ok !!!");
        }

        GridTestUtils.runGC();

        assert weakMap.isEmpty() : weakMap;
    }
}