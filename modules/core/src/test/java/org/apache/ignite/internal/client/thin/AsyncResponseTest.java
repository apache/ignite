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

package org.apache.ignite.internal.client.thin;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Thin client async response tests.
 */
public class AsyncResponseTest extends AbstractThinClientTest {
    /** Default timeout value. */
    private static final long TIMEOUT = 100_000L;

    /** */
    private static final int THREADS_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getClientConnectorConfiguration().setThreadPoolSize(THREADS_CNT - 1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /** */
    @Test
    public void testBlockingOps() throws Exception {
        startGrid(0);
        IgniteClient client = startClient(0);
        ClientCache<Object, Object> cache = client.getOrCreateCache(new ClientCacheConfiguration().setName("test")
            .setAtomicityMode(TRANSACTIONAL));

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; i++) {
                try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT)) {
                    cache.put(0, 0);

                    tx.commit();
                }
            }
        }, THREADS_CNT, "tx-thread");

        assertEquals(0, cache.get(0));
    }

    /** */
    @Test
    public void testTransactionalConsistency() throws Exception {
        startGrids(3);
        IgniteClient client = startClient(0, 1, 2);

        ClientCache<Integer, Integer> cache = client.getOrCreateCache(new ClientCacheConfiguration()
            .setName("test")
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(1)
        );

        int iterations = 10_000;
        int keys = 10;

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < iterations; i++) {
                try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT)) {
                    int key1 = ThreadLocalRandom.current().nextInt(keys);
                    int key2 = ThreadLocalRandom.current().nextInt(keys);
                    int sum = ThreadLocalRandom.current().nextInt(100);

                    if (key1 < key2) { // Avoid deadlocks
                        Integer val1 = cache.get(key1);
                        cache.put(key1, (val1 == null ? 0 : val1) - sum);
                        Integer val2 = cache.get(key2);
                        cache.put(key2, (val2 == null ? 0 : val2) + sum);
                    }
                    else {
                        Integer val2 = cache.get(key2);
                        cache.put(key2, (val2 == null ? 0 : val2) + sum);
                        Integer val1 = cache.get(key1);
                        cache.put(key1, (val1 == null ? 0 : val1) - sum);
                    }

                    tx.commit();
                }
            }
        }, THREADS_CNT, "tx-thread");

        int sum = 0;

        for (int i = 0; i < keys; i++) {
            Integer val = cache.get(i);

            if (val != null)
                sum += val;
        }

        assertEquals(0, sum);
    }

    /** */
    @Test
    public void testTransactionalConsistency1() throws Exception {
        startGrids(1);
        IgniteClient client = startClient(0);

        ClientCache<Integer, Integer> cache = client.getOrCreateCache(new ClientCacheConfiguration()
            .setName("test")
            .setAtomicityMode(TRANSACTIONAL)
        );

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; i++) {
                try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT)) {
                    cache.get(0);
                    cache.put(0, 0);
                    cache.get(1);
                    cache.put(1, 0);

                    tx.commit();
                    log.info(">>>> commited");
                }
            }
        }, 2, "tx-thread");
    }

    /** */
    @Test
    public void testTransactionalConsistency2() throws Exception {
        Ignite ignite = startGrids(1);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName("test")
            .setAtomicityMode(TRANSACTIONAL)
        );

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; i++) {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT, 0)) {
                    IgniteFuture<?> fut = cache.getAsync(0);
                    tx.suspend();
                    fut.get();
                    log.info(">>>> get 0");
                    tx.resume();
                    fut = cache.putAsync(0, 0);
                    tx.suspend();
                    fut.get();
                    log.info(">>>> put 0");
                    tx.resume();
                    fut = cache.getAsync(1);
                    tx.suspend();
                    fut.get();
                    log.info(">>>> get 1");
                    tx.resume();
                    fut = cache.putAsync(1, 0);
                    tx.suspend();
                    fut.get();
                    log.info(">>>> put 1");
                    tx.resume();

                    tx.commit();
                    log.info(">>>> commited");
                }
            }
        }, 2, "tx-thread");
    }

    /** */
    @Test
    public void testTransactionalConsistency3() throws Exception {
        Ignite ignite = startGrids(1);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName("test")
            .setAtomicityMode(TRANSACTIONAL)
        );

        Transaction tx0 = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT, 0);
        tx0.suspend();

        Transaction tx1 = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT, 0);
        tx1.suspend();

        IgniteFuture<?> fut;

        tx0.resume();
        fut = cache.getAsync(0);
        tx0.suspend();
        fut.get();
        log.info(">>>> get 0");

        tx1.resume();
        fut = cache.getAsync(0);
        tx1.suspend();
        //fut.get();
        log.info(">>>> get 0");

        tx0.resume();
        fut = cache.putAsync(0, 0);
        tx0.suspend();
        fut.get();
        log.info(">>>> put 0");

        tx0.resume();
        tx0.commit();

        tx1.resume();
        tx1.commit();
        log.info(">>>> commited");
    }

    /** */
    @Test
    public void testTransactionalConsistency4() throws Exception {
        Ignite ignite = startGrids(1);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName("test")
            .setAtomicityMode(TRANSACTIONAL)
        );

        IgniteFuture<?> fut;

        Transaction tx0 = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT, 0);
        fut = cache.getAsync(0);
    }

}
