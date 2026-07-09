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
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
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
    /** Transaction manager. */
    private static TransactionManager txMgr;

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

        txMgr = new TransactionManagerImple();

        startGrid();
    }

    /**
     * Test for switching tx context by JTA Manager.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJtaTxContextSwitch() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            TransactionConfiguration cfg = grid().context().config().getTransactionConfiguration();

            cfg.setDefaultTxConcurrency(txConcurrency);
            cfg.setDefaultTxIsolation(isolation);

            TransactionManager jtaTm = txMgr;

            IgniteCache<Integer, String> cache = jcache();

            assertNull(grid().transactions().tx());

            jtaTm.begin();

            Transaction tx1 = jtaTm.getTransaction();

            cache.put(1, Integer.toString(1));

            assertNotNull(grid().transactions().tx());

            assertEquals(ACTIVE, grid().transactions().tx().state());

            assertEquals(Integer.toString(1), cache.get(1));

            org.apache.ignite.transactions.Transaction igniteTx = grid().transactions().tx();

            jtaTm.suspend();

            // Narayana's TransactionManagerImple.suspend() only detaches the JTA transaction from the
            // current thread context without calling XAResource.end(xid, TMSUSPEND) on enlisted
            // XA resources. This behavior is fully compliant with the JTA specification: the spec
            // defines TransactionManager.suspend() as "suspend the current transaction and return
            // it" without requiring end() callbacks on XA resources.
            // The previous JTA provider (JOTM) called end(TMSUSPEND) as an implementation detail
            // (not because the spec required it), which is why this test worked out of the box
            // with JOTM. To compensate for Narayana's spec-compliant behavior, we must explicitly
            // suspend the Ignite transaction to detach it from the thread.
            igniteTx.suspend();

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

            // Narayana's TransactionManagerImple.resume() only reattaches the JTA transaction to the
            // current thread context without calling XAResource.start(xid, TMRESUME) on previously
            // enlisted XA resources. This is also fully compliant with the JTA specification:
            // TransactionManager.resume() is defined as "resume a previously suspended transaction"
            // without requiring start() callbacks on XA resources.
            // Similarly to suspend(), JOTM called start(TMRESUME) as an implementation detail,
            // not because the spec required it. To compensate for Narayana's behavior, we must
            // explicitly resume the Ignite transaction to reattach it to the thread.
            igniteTx.resume();

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
    @Test
    public void testJtaTxContextSwitchWithExistingTx() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            TransactionConfiguration cfg = grid().context().config().getTransactionConfiguration();

            cfg.setDefaultTxConcurrency(txConcurrency);
            cfg.setDefaultTxIsolation(isolation);

            TransactionManager jtaTm = txMgr;

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
            return txMgr;
        }
    }
}
