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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheTxPreloadNoWriteTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(REPLICATED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 100));
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxNoWrite() throws Exception {
        txNoWrite(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxNoWriteRollback() throws Exception {
        txNoWrite(false);
    }

    /**
     * @param commit {@code True} if commit transaction.
     * @throws Exception If failed.
     */
    private void txNoWrite(boolean commit) throws Exception {
        Ignite ignite0 = startGrid(0);

        Affinity<Integer> aff = ignite0.affinity(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Object> cache0 = ignite0.cache(DEFAULT_CACHE_NAME);

        try (IgniteDataStreamer<Integer, Object> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 1000; i++)
                streamer.addData(i + 10000, new byte[1024]);
        }

        Ignite ignite1 = startGrid(1);

        Integer key = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        // Want test scenario when ignite1 is new primary node, but ignite0 is still partition owner.
        assertTrue(aff.isPrimary(ignite1.cluster().localNode(), key));

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache0.get(key);

            if (commit)
                tx.commit();
        }

        GridCacheAdapter cacheAdapter = ((IgniteKernal)ignite(0)).context().cache().internalCache(DEFAULT_CACHE_NAME);

        // Check all transactions are finished.
        assertEquals(0, cacheAdapter.context().tm().idMapSize());

        // Try to start one more node.
        startGrid(2);
    }
}
