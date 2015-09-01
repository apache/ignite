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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Simple cache test.
 */
public class IgniteTxTimeoutAbstractTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Grid instances. */
    private static final List<Ignite> IGNITEs = new ArrayList<>();

    /** Transaction timeout. */
    private static final long TIMEOUT = 50;

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_COUNT; i++)
            IGNITEs.add(startGrid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        IGNITEs.clear();
    }

    /**
     * @param i Grid index.
     * @return Cache.
     */
    @Override protected <K, V> IgniteCache<K, V> jcache(int i) {
        return IGNITEs.get(i).cache(null);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommitted() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializable() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommitted() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializable() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws IgniteCheckedException If test failed.
     */
    private void checkTransactionTimeout(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {

        int idx = RAND.nextInt(GRID_COUNT);

        IgniteCache<Integer, String> cache = jcache(idx);

        Transaction tx = ignite(idx).transactions().txStart(concurrency, isolation, TIMEOUT, 0);

        try {
            info("Storing value in cache [key=1, val=1]");

            cache.put(1, "1");

            long sleep = TIMEOUT * 2;

            info("Going to sleep for (ms): " + sleep);

            Thread.sleep(sleep);

            info("Storing value in cache [key=1, val=2]");

            cache.put(1, "2");

            info("Committing transaction: " + tx);

            tx.commit();

            assert false : "Timeout never happened for transaction: " + tx;
        }
        catch (CacheException e) {
            if (!(e.getCause() instanceof TransactionTimeoutException))
                throw e;

            info("Received expected timeout exception [msg=" + e.getMessage() + ", tx=" + tx + ']');
        }
        finally {
            tx.close();
        }
    }
}