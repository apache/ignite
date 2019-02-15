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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests entries distribution between primary-backup-near caches according to nodes count in grid.
 */
@RunWith(JUnit4.class)
public class GridCacheNearEvictionSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private int gridCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setBackups(1);
        cc.setRebalanceMode(SYNC);
        cc.setAtomicityMode(atomicityMode());

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        cc.setNearConfiguration(nearCfg);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** @throws Exception If failed. */
    @Test
    public void testNearEnabledOneNode() throws Exception {
        gridCnt = 1;

        startGridsMultiThreaded(gridCnt);

        try {
            IgniteCache<Integer, String> c = grid(0).cache(DEFAULT_CACHE_NAME);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                c.put(i, Integer.toString(i));

            assertEquals(cnt, c.size());
            assertEquals(cnt, c.size());
            assertEquals(0, near(0).nearSize());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testNearEnabledTwoNodes() throws Exception {
        gridCnt = 2;

        startGridsMultiThreaded(gridCnt);

        try {
            final int cnt = 100;

            grid(0).compute().broadcast(new IgniteCallable<Object>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object call() throws Exception {
                    IgniteCache<Integer, String> c = ignite.cache(DEFAULT_CACHE_NAME);

                    for (int i = 0; i < cnt; i++)
                        c.put(i, Integer.toString(i));

                    return true;
                }
            });

            for (int i = 0; i < gridCnt; i++) {
                assertEquals(cnt, internalCache(i).size());
                assertEquals(0, near(i).nearSize());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testNearEnabledThreeNodes() throws Exception {
        gridCnt = 3;

        startGridsMultiThreaded(gridCnt);

        try {
            final int cnt = 100;

            grid(0).compute().broadcast(new IgniteCallable<Object>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object call() throws Exception {
                    IgniteCache<Integer, String> c = ignite.cache(DEFAULT_CACHE_NAME);

                    for (int i = 0; i < cnt; i++)
                        c.put(i, Integer.toString(i));

                    return true;
                }
            });

            for (int i = 0; i < gridCnt; i++) {
                final GridCacheAdapter cache = internalCache(i);
                final GridCacheAdapter near =  near(i);

                // Repeatedly check cache sizes because of concurrent cache updates.
                assertTrue(GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        // Every node contains either near, backup, or primary.
                        return cnt == cache.size() + near.nearSize();
                    }
                }, getTestTimeout()));

                int keySize = near(i).nearSize();

                assert keySize < cnt : "Key size is not less than count [cnt=" + cnt + ", size=" + keySize + ']';
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
