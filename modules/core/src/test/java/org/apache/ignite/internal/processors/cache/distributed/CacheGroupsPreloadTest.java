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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class CacheGroupsPreloadTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String GROUP1 = "group1";

    /** */
    private static final String GROUP2 = "group2";

    /** */
    private CacheAtomicityMode atomicityMode = CacheAtomicityMode.ATOMIC;

    /** */
    private CacheMode cacheMode = CacheMode.PARTITIONED;

    /** */
    private boolean sameGrp = true;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cfg1 = defaultCacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP1)
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setBackups(1);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2);

        if (!sameGrp)
            cfg2.setGroupName(GROUP2);

        cfg.setCacheConfiguration(cfg1, cfg2);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload1() throws Exception {
        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload2() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testCachePreloadMvcc2() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload3() throws Exception {
        cacheMode = CacheMode.REPLICATED;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload4() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreloadMvcc4() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload5() throws Exception {
        sameGrp = false;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload6() throws Exception {
        sameGrp = false;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testCachePreloadMvcc6() throws Exception {
        sameGrp = false;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload7() throws Exception {
        sameGrp = false;
        cacheMode = CacheMode.REPLICATED;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreload8() throws Exception {
        sameGrp = false;
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachePreloadMvcc8() throws Exception {
        sameGrp = false;
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

        cachePreloadTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void cachePreloadTest() throws Exception {
        IgniteCache<Object, Object> cache = startGrid(0).cache(CACHE1);

        for (int i = 0; i < 1000; i++)
            cache.put(i, CACHE1 + "-" + i);

        cache = startGrid(1).cache(CACHE1);

        for (int i = 0; i < 1000; i++)
            assertEquals(CACHE1 + "-" + i, cache.get(i));

        cache = ignite(1).cache(CACHE2);

        for (int i = 0; i < 1000; i++)
            cache.put(i, CACHE2 + "-" + i);

        cache = startGrid(2).cache(CACHE1);

        for (int i = 0; i < 1000; i++)
            assertEquals(CACHE1 + "-" + i, cache.get(i));

        cache = ignite(2).cache(CACHE2);

        for (int i = 0; i < 1000; i++)
            assertEquals(CACHE2 + "-" + i, cache.get(i));
    }
}
