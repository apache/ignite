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

import java.util.Arrays;
import java.util.Collection;
import jakarta.transaction.Status;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.internal.processors.cache.jta.NarayanaTransactionManagerWrapper;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/**
 * JTA Tx Manager test.
 */
@RunWith(Parameterized.class)
public class GridJtaTransactionManagerSelfTest extends GridCommonAbstractTest {
    /** Narayana TransactionManager (wrapped for proper suspend/resume). */
    private static NarayanaTransactionManagerWrapper narayanaTm;

    /** Narayana UserTransaction. */
    private static UserTransaction narayanaUt;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "txConcurrency={0}")
    public static Collection parameters() {
        return Arrays.asList(
            new TransactionConcurrency[] {OPTIMISTIC},
            new TransactionConcurrency[] {PESSIMISTIC}
        );
    }

    /** Tx concurrency. */
    @Parameterized.Parameter
    public TransactionConcurrency txConcurrency;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName).
            setCacheConfiguration(defaultCacheConfiguration().setCacheMode(PARTITIONED));

        cfg.getTransactionConfiguration().setTxManagerFactory(new TestTxManagerFactory());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        narayanaTm = new NarayanaTransactionManagerWrapper(
            new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple());
        narayanaUt = new com.arjuna.ats.internal.jta.transaction.arjunacore.UserTransactionImple();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (narayanaTm != null) {
            Transaction tx = narayanaTm.getTransaction();
            if (tx != null && tx.getStatus() == Status.STATUS_ACTIVE)
                narayanaTm.rollback();
        }
    }

    /**
     * Narayana 7.x does not support suspend/resume like JOTM.
     * No open-source JTA implementation with Jakarta API provides JOTM-style suspend/resume.
     * Atomikos 6.x is commercial, Bitronix is dead (no Jakarta support).
     */
    @Ignore("Narayana 7.x does not support JOTM-style suspend/resume")
    @Test
    public void testJtaTxContextSwitch() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            TransactionConfiguration cfg = grid().context().config().getTransactionConfiguration();

            cfg.setDefaultTxConcurrency(txConcurrency);
            cfg.setDefaultTxIsolation(isolation);

            TransactionManager jtaTm = narayanaTm;

            IgniteCache<Integer, String> cache = jcache();

            assertNull(grid().transactions().tx());

            jtaTm.begin();

            Transaction tx1 = jtaTm.getTransaction();

            cache.put(1, Integer.toString(1));

            assertNotNull(grid().transactions().tx());

            assertEquals(ACTIVE, grid().transactions().tx().state());

            assertEquals(Integer.toString(1), cache.get(1));

            jtaTm.suspend();

            assertNull(grid().transactions().tx());

            assertNull(cache.get(1));

            jtaTm.begin();

            Transaction tx2 = jtaTm.getTransaction();

            assertNotSame(tx1, tx2);

            cache.put(2, Integer.toString(2));

            assertNotNull(grid().transactions().tx());

            assertEquals(ACTIVE, grid().transactions().tx().state());

            assertEquals(Integer.toString(2), cache.get(2));

            jtaTm.commit();

            assertNull(grid().transactions().tx());

            assertEquals(Integer.toString(2), cache.get(2));

            jtaTm.resume(tx1);

            assertNotNull(grid().transactions().tx());

            assertEquals(ACTIVE, grid().transactions().tx().state());

            cache.put(3, Integer.toString(3));

            jtaTm.commit();

            assertEquals("1", cache.get(1));
            assertEquals("2", cache.get(2));
            assertEquals("3", cache.get(3));

            assertNull(grid().transactions().tx());

            cache.removeAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    /**
     * Narayana 7.x does not support suspend/resume like JOTM.
     * No open-source JTA implementation with Jakarta API provides JOTM-style suspend/resume.
     * Atomikos 6.x is commercial, Bitronix is dead (no Jakarta support).
     */
    @Ignore("Narayana 7.x does not support JOTM-style suspend/resume")
    @Test
    public void testJtaTxContextSwitchWithExistingTx() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            TransactionConfiguration cfg = grid().context().config().getTransactionConfiguration();

            cfg.setDefaultTxConcurrency(txConcurrency);
            cfg.setDefaultTxIsolation(isolation);

            TransactionManager jtaTm = narayanaTm;

            IgniteCache<Integer, String> cache = jcache();

            jtaTm.begin();

            Transaction tx1 = jtaTm.getTransaction();

            cache.put(1, Integer.toString(1));

            assertNotNull(grid().transactions().tx());

            assertEquals(ACTIVE, grid().transactions().tx().state());

            assertEquals(Integer.toString(1), cache.get(1));

            jtaTm.suspend();

            jtaTm.begin();

            Transaction tx2 = jtaTm.getTransaction();

            assertNotSame(tx1, tx2);

            cache.put(2, Integer.toString(2));

            try {
                jtaTm.resume(tx1);

                fail("jtaTm.resume shouldn't success.");
            }
            catch (IllegalStateException ignored) {
                // No-op.
            }
            finally {
                jtaTm.rollback(); //rolling back tx2
            }

            jtaTm.resume(tx1);
            jtaTm.rollback();

            cache.removeAll();
        }
    }

    /**
     *
     */
    static class TestTxManagerFactory implements Factory<TransactionManager> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public TransactionManager create() {
            return narayanaTm;
        }
    }
}
