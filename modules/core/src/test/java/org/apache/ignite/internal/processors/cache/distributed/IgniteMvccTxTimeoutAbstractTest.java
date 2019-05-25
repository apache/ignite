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

import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Simple cache test.
 */
@Ignore("https://issues.apache.org/jira/browse/IGNITE-7388")
public class IgniteMvccTxTimeoutAbstractTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Transaction timeout. */
    private static final long TIMEOUT = 50;

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_COUNT, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TransactionConfiguration txCfg = c.getTransactionConfiguration();

        txCfg.setDefaultTxTimeout(TIMEOUT);

        return c;
    }

    /**
     * @param i Grid index.
     * @return Cache.
     */
    @Override protected <K, V> IgniteCache<K, V> jcache(int i) {
        return grid(i).cache(DEFAULT_CACHE_NAME);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, REPEATABLE_READ);
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
        catch (Exception e) {
            if (!(X.hasCause(e, TransactionTimeoutException.class)))
                throw e;

            info("Received expected timeout exception [msg=" + e.getMessage() + ", tx=" + tx + ']');
        }
        finally {
            tx.close();
        }
    }
}
