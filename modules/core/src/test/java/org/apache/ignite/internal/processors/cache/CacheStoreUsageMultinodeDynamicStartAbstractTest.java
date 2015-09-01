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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;

/**
 *
 */
public abstract class CacheStoreUsageMultinodeDynamicStartAbstractTest extends CacheStoreUsageMultinodeAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cache = false;

        startGridsMultiThreaded(3);

        client = true;

        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStart() throws Exception {
        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartNearEnabled() throws Exception {
        nearCache = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicFromClientStart() throws Exception {
        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartFromClientNearEnabled() throws Exception {
        nearCache = true;

        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartLocalStore() throws Exception {
        locStore = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartFromClientLocalStore() throws Exception {
        locStore = true;

        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartLocalStoreNearEnabled() throws Exception {
        locStore = true;

        nearCache = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartWriteBehindStore() throws Exception {
        writeBehind = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartFromClientWriteBehindStore() throws Exception {
        writeBehind = true;

        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicStartWriteBehindStoreNearEnabled() throws Exception {
        writeBehind = true;

        nearCache = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @param clientStart {@code True} if start cache from client node.
     * @throws Exception If failed.
     */
    private void checkStoreWithDynamicStart(boolean clientStart) throws Exception {
        cacheStore = true;

        CacheConfiguration ccfg = cacheConfiguration();

        assertNotNull(ccfg.getCacheStoreFactory());

        Ignite srv = ignite(0);

        Ignite client = ignite(3);

        Ignite node = clientStart ? client : srv;

        IgniteCache cache = nearCache ? node.createCache(ccfg, new NearCacheConfiguration()) : node.createCache(ccfg);

        assertNotNull(cache);

        try {
            if (nearCache)
                client.createNearCache(null, new NearCacheConfiguration<>());

            checkStoreUpdate(true);
        }
        finally {
            cache = srv.cache(null);

            if (cache != null)
                cache.destroy();
        }
    }
}