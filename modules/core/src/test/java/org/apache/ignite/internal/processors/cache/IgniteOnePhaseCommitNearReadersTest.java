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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;

/**
 *
 */
public class IgniteOnePhaseCommitNearReadersTest extends GridCommonAbstractTest {
    /** */
    private boolean testSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (testSpi) {
            TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

            cfg.setCommunicationSpi(commSpi);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutReadersUpdate1() throws Exception {
        putReadersUpdate(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutReadersUpdate2() throws Exception {
        putReadersUpdate(0);
    }

    /**
     * @param backups Backups number.
     * @throws Exception If failed.
     */
    private void putReadersUpdate(int backups) throws Exception {
        final int SRVS = 3;

        startGrids(SRVS);

        awaitPartitionMapExchange();

        Ignite srv = ignite(0);

        srv.createCache(cacheConfiguration(backups));

        Ignite client1 = startClientGrid(SRVS);

        IgniteCache<Object, Object> cache1 = client1.createNearCache(DEFAULT_CACHE_NAME,
            new NearCacheConfiguration<>());

        Integer key = primaryKey(srv.cache(DEFAULT_CACHE_NAME));

        Ignite client2 = startClientGrid(SRVS + 1);

        IgniteCache<Object, Object> cache2 = client2.cache(DEFAULT_CACHE_NAME);

        cache1.put(key, 1);

        cache2.put(key, 2);

        checkCacheData(F.asMap(key, 2), DEFAULT_CACHE_NAME);

        int val = 10;

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                try (Transaction tx = client2.transactions().txStart(concurrency, isolation)) {
                    cache2.put(key, val);

                    tx.commit();
                }

                checkCacheData(F.asMap(key, val), DEFAULT_CACHE_NAME);

                val++;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutReaderUpdatePrimaryFails1() throws Exception {
        putReaderUpdatePrimaryFails(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutReaderUpdatePrimaryFails2() throws Exception {
        putReaderUpdatePrimaryFails(0);
    }

    /**
     * @param backups Backups number.
     * @throws Exception If failed.
     */
    private void putReaderUpdatePrimaryFails(int backups) throws Exception {
        testSpi = true;

        final int SRVS = 3;

        startGrids(SRVS);

        awaitPartitionMapExchange();

        Ignite srv = ignite(0);

        srv.createCache(cacheConfiguration(backups));

        Ignite client1 = startClientGrid(SRVS);

        IgniteCache<Object, Object> cache1 = client1.createNearCache(DEFAULT_CACHE_NAME,
            new NearCacheConfiguration<>());

        Ignite client2 = startClientGrid(SRVS + 1);

        IgniteCache<Object, Object> cache2 = client2.cache(DEFAULT_CACHE_NAME);

        Integer key = primaryKey(srv.cache(DEFAULT_CACHE_NAME));

        cache1.put(key, 1);

        spi(srv).blockMessages(GridNearTxPrepareResponse.class, client2.name());

        IgniteFuture<?> fut = cache2.putAsync(key, 2);

        U.sleep(1000);

        assertFalse(fut.isDone());

        stopGrid(0);

        fut.get();

        checkCacheData(F.asMap(key, backups == 0 ? null : 2), DEFAULT_CACHE_NAME);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                srv = startGrid(0);

                awaitPartitionMapExchange(true, true, null);

                key = primaryKey(srv.cache(DEFAULT_CACHE_NAME));

                cache1.put(key, 1);

                spi(srv).blockMessages(GridNearTxPrepareResponse.class, client2.name());

                try (Transaction tx = client2.transactions().txStart(concurrency, isolation)) {
                    cache2.putAsync(key, 2);

                    fut = tx.commitAsync();

                    U.sleep(1000);

                    assertFalse(fut.isDone());

                    stopGrid(0);

                    if (backups == 0)
                        fut.get();
                    else {
                        try {
                            fut.get();

                            fail();
                        }
                        catch (TransactionRollbackException ignore) {
                            // Expected.
                        }
                    }
                }

                checkCacheData(F.asMap(key, backups == 0 ? null : 1), DEFAULT_CACHE_NAME);

                try (Transaction tx = client2.transactions().txStart(concurrency, isolation)) {
                    cache2.putAsync(key, 2);

                    tx.commit();
                }

                checkCacheData(F.asMap(key, 2), DEFAULT_CACHE_NAME);
            }
        }
    }

    /**
     * @param backups Backups number.
     * @return Configuration.
     */
    private CacheConfiguration cacheConfiguration(int backups) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);

        return ccfg;
    }
}
