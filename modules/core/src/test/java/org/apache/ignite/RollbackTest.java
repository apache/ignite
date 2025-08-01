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

package org.apache.ignite;

import java.util.function.Consumer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
public class RollbackTest extends GridCommonAbstractTest {
    /** Tx timeout. */
    public static final int TX_TIMEOUT = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(2)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testSimpleRollback() throws Exception {
        IgniteEx ignite = startGrids(3);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        IgniteTransactions transactions = ignite.transactions();

        Transaction tx = transactions.txStart(PESSIMISTIC, REPEATABLE_READ, TX_TIMEOUT, 0);

        try {
            cache.put(2, 2);

            tx.rollback();
        }
        catch (Exception e) {
            log.warning(">>>>>> Exception occurred", e);
        }

        assertNull(transactions.tx());

        assertNull(cache.get(2));

        int val = cache.get(1);

        assertEquals(1, val);
    }

    /** */
    @Test
    public void testTimeoutBeforeFirstAction() throws Exception {
        doTest(cache -> {
            doSleep(TX_TIMEOUT * 2);

            cache.put(2, 2);
        });
    }

    /** */
    @Test
    public void testTimeoutInTheMiddleOfTransaction() throws Exception {
        doTest(cache -> {
            cache.put(2, 2);

            doSleep(TX_TIMEOUT * 2);

            cache.put(3, 3);
        });
    }

    /** */
    @Test
    public void testTimeoutBeforeCommit() throws Exception {
        doTest(cache -> {
            cache.put(2, 2);

            doSleep(TX_TIMEOUT * 2);
        });
    }

    /** */
    private void doTest(Consumer<IgniteCache<Integer, Integer>> cnsmr) throws Exception {
        IgniteEx ignite = startGrids(3);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        IgniteTransactions transactions = ignite.transactions();

        Transaction tx = transactions.txStart(PESSIMISTIC, REPEATABLE_READ, TX_TIMEOUT, 0);

        try {
            cnsmr.accept(cache);

            tx.commit();
        }
        catch (Exception e) {
            log.warning(">>>>>> Rolling back", e);

            try {
                tx.rollback();
            }
            catch (Exception e2) {
                log.warning(">>>>>> Rollback failed", e2);
            }
        }

        assertNull(transactions.tx());

        doSleep(TX_TIMEOUT * 2);

        int val = cache.get(1);

        assertEquals(1, val);
    }
}
