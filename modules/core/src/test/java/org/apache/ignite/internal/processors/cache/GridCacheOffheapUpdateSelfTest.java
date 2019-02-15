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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Check for specific support issue.
 */
public class GridCacheOffheapUpdateSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setNearConfiguration(null);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateInPessimisticTxOnRemoteNode() throws Exception {
        try {
            Ignite ignite = startGrids(2);

            IgniteCache<Object, Object> rmtCache = ignite.cache(DEFAULT_CACHE_NAME);

            int key = 0;

            while (!ignite.affinity(DEFAULT_CACHE_NAME).isPrimary(grid(1).localNode(), key))
                key++;

            IgniteCache<Object, Object> locCache = grid(1).cache(DEFAULT_CACHE_NAME);

            try (Transaction tx = grid(1).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                locCache.putIfAbsent(key, 0);

                tx.commit();
            }

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(0, rmtCache.get(key));

                rmtCache.put(key, 1);

                tx.commit();
            }

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(1, rmtCache.get(key));

                rmtCache.put(key, 2);

                tx.commit();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadEvictedPartition() throws Exception {
        try {
            Ignite grid = startGrid(0);

            IgniteCache<Object, Object> cache = grid.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 30; i++)
                cache.put(i, 0);

            startGrid(1);

            awaitPartitionMapExchange();

            for (int i = 0; i < 30; i++)
                grid(1).cache(DEFAULT_CACHE_NAME).put(i, 10);

            // Find a key that does not belong to started node anymore.
            int key = 0;

            ClusterNode locNode = grid.cluster().localNode();

            for (;key < 30; key++) {
                if (!grid.affinity(DEFAULT_CACHE_NAME).isPrimary(locNode, key) && !grid.affinity(DEFAULT_CACHE_NAME).isBackup(locNode, key))
                    break;
            }

            assertEquals(10, cache.get(key));

            if(((IgniteCacheProxy)cache).context().config().getAtomicityMode() != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT) {
                try (Transaction ignored = grid.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                    assertEquals(10, cache.get(key));
                }

                try (Transaction ignored = grid.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    assertEquals(10, cache.get(key));
                }
            }
            else {
                try (Transaction ignored = grid.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    assertEquals(10, cache.get(key));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
