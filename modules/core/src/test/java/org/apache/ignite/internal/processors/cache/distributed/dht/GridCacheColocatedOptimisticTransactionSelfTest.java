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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test ensuring that values are visible inside OPTIMISTIC transaction in co-located cache.
 */
@RunWith(JUnit4.class)
public class GridCacheColocatedOptimisticTransactionSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Cache name. */
    private static final String CACHE = "cache";

    /** Key. */
    private static final Integer KEY = 1;

    /** Value. */
    private static final String VAL = "val";

    /** Grids. */
    private static Ignite[] ignites;

    /** Regular caches. */
    private static IgniteCache<Integer, String>[] caches;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cc.setName(CACHE);
        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(null);
        cc.setBackups(1);
        cc.setWriteSynchronizationMode(FULL_SYNC);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        ignites = new Ignite[GRID_CNT];
        caches = new IgniteCache[GRID_CNT];

        for (int i = 0; i < GRID_CNT; i++) {
            ignites[i] = startGrid(i);

            caches[i] = ignites[i].cache(CACHE);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        caches = null;
        ignites = null;
    }

    /**
     * Perform test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTransaction() throws Exception {
        for (IgniteCache<Integer, String> cache : caches) {
            Transaction tx = cache.unwrap(Ignite.class).transactions().txStart(OPTIMISTIC, REPEATABLE_READ);

            try {
                cache.put(KEY, VAL);

                tx.commit();
            }
            finally {
                tx.close();
            }

            for (IgniteCache<Integer, String> cacheInner : caches) {
                tx = cacheInner.unwrap(Ignite.class).transactions().txStart(OPTIMISTIC, REPEATABLE_READ);

                try {
                    assert F.eq(VAL, cacheInner.get(KEY));

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }

            tx = cache.unwrap(Ignite.class).transactions().txStart(OPTIMISTIC, REPEATABLE_READ);

            try {
                cache.remove(KEY);

                tx.commit();
            }
            finally {
                tx.close();
            }
        }
    }
}
