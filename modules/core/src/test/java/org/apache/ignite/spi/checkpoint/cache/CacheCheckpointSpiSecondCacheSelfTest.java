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

package org.apache.ignite.spi.checkpoint.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for cache checkpoint SPI with second cache configured.
 */
@RunWith(JUnit4.class)
public class CacheCheckpointSpiSecondCacheSelfTest extends GridCommonAbstractTest {
    /** Checkpoints cache name. */
    private static final String CP_CACHE = "checkpoints";

    /** Starts grid. */
    public CacheCheckpointSpiSecondCacheSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName(DEFAULT_CACHE_NAME);
        cacheCfg1.setCacheMode(REPLICATED);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);

        CacheConfiguration cacheCfg2 = defaultCacheConfiguration();

        cacheCfg2.setName(CP_CACHE);
        cacheCfg2.setCacheMode(REPLICATED);
        cacheCfg2.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        CacheCheckpointSpi cp = new CacheCheckpointSpi();

        cp.setCacheName(CP_CACHE);

        cfg.setCheckpointSpi(cp);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSecondCachePutRemove() throws Exception {
        IgniteCache<Integer, Integer> data = grid().cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, String> cp = grid().cache(CP_CACHE);

        data.put(1, 1);
        cp.put(1, "1");

        Integer v = data.get(1);

        assertNotNull(v);
        assertEquals(Integer.valueOf(1), data.get(1));

        data.remove(1);

        assertNull(data.get(1));

        assertTrue(data.localSize() == 0);

        assertEquals(1, cp.size());
        assertEquals("1", cp.get(1));

    }
}
