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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@RunWith(JUnit4.class)
public class TxMultiCacheAsyncOpsTest extends GridCommonAbstractTest {
    /** Grid count. */
    public static final int GRID_COUNT = 3;

    /** Caches count. */
    public static final int CACHES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfigurations());

        return cfg;
    }

    /** */
    private CacheConfiguration[] cacheConfigurations() {
        return IntStream.range(0, CACHES_CNT).mapToObj(
            this::cacheConfiguration).collect(Collectors.toList()).toArray(new CacheConfiguration[CACHES_CNT]);
    }

    /**
     * @param idx Index.
     */
    private CacheConfiguration cacheConfiguration(int idx) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME + idx);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(false);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_COUNT);
    }

    /**
     *
     */
    @Test
    public void testCommitAfterAsyncPut() {
        CacheConfiguration[] caches = cacheConfigurations();

        try {
            for (int i = 0; i < caches.length; i++)
                grid(0).cache(caches[i].getName()).put(1, i + 1);

            try (Transaction tx = grid(0).transactions().txStart()) {
                for (int i = 0; i < caches.length; i++)
                    grid(0).cache(caches[i].getName()).putAsync(1, (i + 1) * 10);

                tx.commit();
            }
            catch (Exception e) {
                System.out.println();
            }

            for (int i = 0; i < caches.length; i++)
                assertEquals((i + 1) * 10, grid(0).cache(caches[i].getName()).get(1));
        }
        finally {
            for (int i = 0; i < caches.length; i++)
                grid(0).cache(caches[i].getName()).removeAll();
        }
    }

    /**
     *
     */
    @Test
    public void testCommitAfterAsyncGet() {
        CacheConfiguration[] caches = cacheConfigurations();

        try {
            for (int i = 0; i < caches.length; i++)
                grid(0).cache(caches[i].getName()).put(1, i + 1);

            List<IgniteFuture> futs = new ArrayList<>();

            try (Transaction tx = grid(0).transactions().txStart()) {
                for (int i = 0; i < caches.length; i++)
                    futs.add(grid(0).cache(caches[i].getName()).getAsync(1));

                tx.commit();
            }

            for (int i = 0; i < futs.size(); i++)
                assertEquals(i + 1, futs.get(i).get());
        }
        finally {
            for (int i = 0; i < caches.length; i++)
                grid(0).cache(caches[i].getName()).removeAll();
        }
    }
}
