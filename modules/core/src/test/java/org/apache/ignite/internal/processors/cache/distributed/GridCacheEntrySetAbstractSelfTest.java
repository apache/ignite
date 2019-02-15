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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@RunWith(JUnit4.class)
public abstract class GridCacheEntrySetAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final String TX_KEY = "txKey";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected CacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntrySet() throws Exception {
        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            final AtomicInteger cacheIdx = new AtomicInteger(0);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = cacheIdx.getAndIncrement();

                    log.info("Use cache " + idx);

                    IgniteCache<Object, Object> cache = grid(idx).cache(DEFAULT_CACHE_NAME);

                    for (int i = 0; i < 100; i++)
                        putAndCheckEntrySet(cache);

                    return null;
                }
            }, GRID_CNT, "test");

            for (int j = 0; j < gridCount(); j++)
                jcache(j).removeAll();
        }
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void putAndCheckEntrySet(IgniteCache<Object, Object> cache) throws Exception {
        try (Transaction tx = cache.unwrap(Ignite.class).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Integer total = (Integer) cache.get(TX_KEY);

            if (total == null)
                total = 0;

            int cntr = 0;

            for (Cache.Entry e : cache) {
                if (e.getKey() instanceof Integer)
                    cntr++;
            }

            assertEquals(total, (Integer)cntr);

            cache.put(cntr + 1, cntr + 1);

            cache.put(TX_KEY, cntr + 1);

            tx.commit();
        }
    }
}
