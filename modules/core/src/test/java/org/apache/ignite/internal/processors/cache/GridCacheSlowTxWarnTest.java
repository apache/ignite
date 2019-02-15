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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test to check slow TX warning timeout defined by
 * {@link org.apache.ignite.IgniteSystemProperties#IGNITE_SLOW_TX_WARN_TIMEOUT}
 * system property.
 */
public class GridCacheSlowTxWarnTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc1 = defaultCacheConfiguration();

        cc1.setName("partitioned");
        cc1.setCacheMode(PARTITIONED);
        cc1.setBackups(1);

        CacheConfiguration cc2 = defaultCacheConfiguration();

        cc2.setName("replicated");
        cc2.setCacheMode(REPLICATED);

        CacheConfiguration cc3 = defaultCacheConfiguration();

        cc3.setName("local");
        cc3.setCacheMode(LOCAL);

        c.setCacheConfiguration(cc1, cc2, cc3);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWarningOutput() throws Exception {
        try {
            IgniteKernal g = (IgniteKernal)startGrid(1);

            info(">>> Slow tx timeout is not set, long-live txs simulated.");

            checkCache(g, "partitioned", true, false);
            checkCache(g, "replicated", true, false);
            checkCache(g, "local", true, false);

            info(">>> Slow tx timeout is set, long-live tx simulated.");

            checkCache(g, "partitioned", true, true);
            checkCache(g, "replicated", true, true);
            checkCache(g, "local", true, true);

            info(">>> Slow tx timeout is set, no long-live txs.");

            checkCache(g, "partitioned", false, true);
            checkCache(g, "replicated", false, true);
            checkCache(g, "local", false, true);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param g Grid.
     * @param cacheName Cache.
     * @param simulateTimeout Simulate timeout.
     * @param configureTimeout Alter configuration of TX manager.
     * @throws Exception If failed.
     */
    private void checkCache(Ignite g, String cacheName, boolean simulateTimeout,
        boolean configureTimeout) throws Exception {
        if (configureTimeout) {
            GridCacheAdapter<Integer, Integer> cache = ((IgniteKernal)g).internalCache(cacheName);

            cache.context().tm().slowTxWarnTimeout(500);
        }

        IgniteCache<Object, Object> cache1 = g.cache(cacheName);

        Transaction tx = g.transactions().txStart();

        try {
            cache1.put(1, 1);

            if (simulateTimeout)
                Thread.sleep(800);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = g.transactions().txStart();

        try {
            cache1.put(1, 1);

            if (simulateTimeout)
                Thread.sleep(800);

            tx.rollback();
        }
        finally {
            tx.close();
        }
    }
}
