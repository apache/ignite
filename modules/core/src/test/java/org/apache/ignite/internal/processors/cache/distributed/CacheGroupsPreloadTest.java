/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
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
