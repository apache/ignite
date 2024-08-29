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

package org.apache.ignite.internal.processors.query.calcite.rules;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractTransactionalSqlTest extends GridCommonAbstractTest {
    /** */
    public enum TxDml {
        /** All put, remove and SQL dml will be executed inside transaction. */
        ALL,

        /** Only some DML operations will be executed inside transaction. */
        RANDOM,

        /** Don't use transaction for DML. */
        NONE
    }

    /** */
    @Parameterized.Parameter()
    public TxDml txDml;

    /** */
    protected static TxDml currentMode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "txDml={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(TxDml.values());
    }

    /** */
    protected static Transaction tx;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());
        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        if (currentMode != null && txDml == currentMode)
            return;

        currentMode = txDml;

        if (tx != null) {
            tx.resume();

            tx.rollback();

            tx = null;
        }

        stopAllGrids();

        init();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        currentMode = null;

        tx = null;
    }

    /** */
    protected abstract void init() throws Exception;

    /** */
    protected <K, V> void put(Ignite node, IgniteCache<K, V> cache, K key, V val) {
        invokeAction(node, () -> {
            cache.put(key, val);
            return null;
        });
    }

    /** */
    protected <T> T invokeAction(Ignite node, SupplierX<T> action) {
        switch (txDml) {
            case ALL:
                return txAction(node, action);
            case NONE:
                return action.get();
            case RANDOM:
                if (ThreadLocalRandom.current().nextBoolean())
                    return action.get();
                else
                    return txAction(node, action);
            default:
                throw new IllegalArgumentException();
        }
    }

    /** */
    public <T> T txAction(Ignite node, SupplierX<T> action) {
        if (tx == null)
            startTransaction(node);

        tx.resume();

        try {
            return action.get();
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
    protected interface SupplierX<T> {
        /** */
        T getx() throws Exception;

        /** */
        default T get() {
            try {
                return getx();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** */
    protected QueryContext queryContext() {
        return QueryContext.of(tx != null ? ((TransactionProxyImpl)tx).tx().xidVersion() : null);
    }

    /** */
    protected <K, V> CacheConfiguration<K, V> cacheConfiguration() {
        return new CacheConfiguration<K, V>().setAtomicityMode(txDml == TxDml.NONE
            ? CacheAtomicityMode.ATOMIC
            : CacheAtomicityMode.TRANSACTIONAL);
    }
}
