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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheAffinityEarlyTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static int GRID_CNT = 8;

    /** Stopped. */
    private volatile boolean stopped;

    /** Iteration. */
    private static final int iters = 10;

    /** Concurrent. */
    private static final boolean concurrent = true;

    /** Futs. */
    private Collection<IgniteInternalFuture<?>> futs = new ArrayList<>(GRID_CNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 6 * 60 * 1000L;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNodes() throws Exception {
        for (int i = 0; i < iters; i++) {
            try {
                log.info("Iteration: " + (i + 1) + '/' + iters);

                doTest();
            }
            finally {
                stopAllGrids(true);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        final AtomicBoolean failed = new AtomicBoolean();

        for (int i = 0; i < GRID_CNT; i++) {
            final int idx = i;

            final Ignite grid = concurrent ? null : startGrid(idx);

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    Random rnd = new Random();

                    try {
                        Ignite ignite = grid == null ? startGrid(idx) : grid;

                        IgniteCache<Object, Object> cache = getCache(ignite);

                        cache.put(ignite.cluster().localNode().id(), UUID.randomUUID());

                        while (!stopped) {
                            int val = Math.abs(rnd.nextInt(100));

                            if (val >= 0 && val < 40)
                                cache.containsKey(ignite.cluster().localNode().id());
                            else if (val >= 40 && val < 80)
                                cache.get(ignite.cluster().localNode().id());
                            else
                                cache.put(ignite.cluster().localNode().id(), UUID.randomUUID());

                            Thread.sleep(50);
                        }
                    }
                    catch (Exception e) {
                        log.error("Unexpected error: " + e, e);

                        failed.set(true);
                    }
                }
            }, 1);

            futs.add(fut);
        }

        Thread.sleep(10_000);

        stopped = true;

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        assertFalse(failed.get());
    }

    /**
     * @param grid Grid.
     * @return Cache.
     */
    private IgniteCache getCache(Ignite grid) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(null);

        return grid.getOrCreateCache(ccfg);
    }
}
