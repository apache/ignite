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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractTransactionalQueryTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter()
    public TestTransactionMode sqlTxMode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "sqlTxMode={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(TestTransactionMode.values());
    }

    /** */
    protected static TestTransactionMode currentMode;

    /** */
    protected static Transaction tx;


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        if (currentMode != null && sqlTxMode == currentMode)
            return;

        currentMode = sqlTxMode;

        clearTransaction();

        stopAllGrids();

        init();
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clearTransaction();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        clearTransaction();

        stopAllGrids();

        currentMode = null;

        tx = null;

        super.afterTestsStopped();
    }

    /** */
    protected abstract void init() throws Exception;

    /** */
    protected CacheAtomicityMode atomicity() {
        return sqlTxMode == TestTransactionMode.NONE ? ATOMIC : TRANSACTIONAL;
    }

    /** */
    protected <K, V> void put(Ignite node, IgniteCache<K, V> cache, K key, V val) {
        invokeAction(node, () -> cache.put(key, val));
    }

    /** */
    protected void invokeAction(Ignite node, RunnableX action) {
        if (tx == null && sqlTxMode != TestTransactionMode.NONE)
            startTransaction(node);

        switch (sqlTxMode) {
            case ALL:
                txAction(node, action);

                break;
            case NONE:
                action.run();

                break;
            case RANDOM:
                if (ThreadLocalRandom.current().nextBoolean())
                    action.run();
                else
                    txAction(node, action);

                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    /** */
    public void txAction(Ignite node, RunnableX action) {
        tx.resume();

        try {
            action.run();
        }
        finally {
            tx.suspend();
        }
    }

    /** */
    protected void startTransaction(Ignite node) {
        tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED, getTestTimeout(), 100);

        tx.suspend();
    }

    /** */
    protected void clearTransaction() {
        if (tx == null)
            return;

        tx.resume();

        tx.rollback();

        tx = null;
    }

    /** */
    public enum TestTransactionMode {
        /** All put, remove and SQL dml will be executed inside transaction. */
        ALL,

        /** Only some DML operations will be executed inside transaction. */
        RANDOM,

        /** Don't use transaction for DML. */
        NONE
    }
}
