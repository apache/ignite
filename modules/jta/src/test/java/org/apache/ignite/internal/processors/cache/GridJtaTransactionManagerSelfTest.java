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

import javax.cache.configuration.Factory;
import javax.transaction.Status;
import javax.transaction.Transaction;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.objectweb.jotm.Current;
import org.objectweb.jotm.Jotm;
import org.objectweb.transaction.jta.TransactionManager;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/**
 * JTA Tx Manager test.
 */
public class GridJtaTransactionManagerSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /**
     * Java Open Transaction Manager facade.
     */
    private static Jotm jotm;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxManagerFactory(new Factory<TransactionManager>() {
            private static final long serialVersionUID = 0L;

            @Override public TransactionManager create() {

                return jotm.getTransactionManager();
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        jotm = new Jotm(true, false);
        Current.setAppServer(false);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        jotm.stop();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }


    /**
     * Test for switching tx context by JTA Manager.
     *
     * @throws Exception If failed.
     */
    public void testJtaTransactionContextSwitch() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {

                TransactionConfiguration configuration = grid(0).context().config().getTransactionConfiguration();
                configuration.setDefaultTxConcurrency(concurrency);
                configuration.setDefaultTxIsolation(isolation);

                jtaTransactionContextSwitch(TransactionConcurrency.OPTIMISTIC.equals(concurrency)
                    && (TransactionIsolation.REPEATABLE_READ.equals(isolation)));
            }
        }
    }

    /**
     * @param checkRepeatableRead True if repeatable read scenario should be tested.
     * @throws Exception If failed.
     */
    private void jtaTransactionContextSwitch(boolean checkRepeatableRead) throws Exception {
        TransactionManager jtaTm = jotm.getTransactionManager();

        IgniteCache<String, Integer> cache = jcache();

        Integer[] vals = {1, 2, 3};
        String[] keys = {"key1", "key2", "key3"};

        Transaction[] jtaTxs = new Transaction[3];

        try {
            for (int i = 0; i < 3; i++) {
                assertNull(ignite(0).transactions().tx());

                jtaTm.begin();

                jtaTxs[i] = jtaTm.getTransaction();

                assertNull(ignite(0).transactions().tx());
                assertNull(cache.getAndPut(keys[i], vals[i]));

                assertNotNull(ignite(0).transactions().tx());
                assertEquals(ACTIVE, ignite(0).transactions().tx().state());
                assertEquals(vals[i], cache.get(keys[i]));

                if (checkRepeatableRead)
                    for (int j = 0; j < 3; j++)
                        assertEquals(j == i ? vals[j] : null, cache.get(keys[j]));

                jtaTm.suspend();

                assertNull(ignite(0).transactions().tx());
                assertNull(cache.get(keys[i]));
            }

            for (int i = 0; i < 3; i++) {
                jtaTm.resume(jtaTxs[i]);

                assertNotNull(ignite(0).transactions().tx());
                assertEquals(ACTIVE, ignite(0).transactions().tx().state());

                if (checkRepeatableRead)
                    for (int j = 0; j < 3; j++)
                        assertEquals(j == i ? vals[j] : null, cache.get(keys[j]));
                else
                    for (int j = 0; j < 3; j++)
                        assertEquals(j <= i ? vals[j] : null, cache.get(keys[j]));

                jtaTm.commit();

                for (int j = 0; j < 3; j++)
                    assertEquals(j <= i ? vals[j] : null, cache.get(keys[j]));

                assertNull(ignite(0).transactions().tx());
            }

        }
        finally {
            for (int i = 0; i < 3; i++)
                if (jtaTxs[i] != null && jtaTxs[i].getStatus() == Status.STATUS_ACTIVE)
                    jtaTxs[i].rollback();
        }

        for (int i = 0; i < 3; i++)
            assertEquals(vals[i], cache.get(keys[i]));

        cache.removeAll();
    }
}
