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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public abstract class CacheStoreUsageMultinodeDynamicStartAbstractTest extends CacheStoreUsageMultinodeAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cache = false;

        startGridsMultiThreaded(3);

        client = true;

        startGrid(3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStart() throws Exception {
        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStartNearEnabled() throws Exception {
        nearCache = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicFromClientStart() throws Exception {
        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStartFromClientNearEnabled() throws Exception {
        nearCache = true;

        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStartLocalStore() throws Exception {
        locStore = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStartFromClientLocalStore() throws Exception {
        locStore = true;

        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStartLocalStoreNearEnabled() throws Exception {
        locStore = true;

        nearCache = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStartWriteBehindStore() throws Exception {
        writeBehind = true;

        checkStoreWithDynamicStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicStartFromClientWriteBehindStore() throws Exception {
        writeBehind = true;

        checkStoreWithDynamicStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
                client.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

            checkStoreUpdate(true);
        }
        finally {
            cache = srv.cache(DEFAULT_CACHE_NAME);

            if (cache != null)
                cache.destroy();
        }
    }
}
