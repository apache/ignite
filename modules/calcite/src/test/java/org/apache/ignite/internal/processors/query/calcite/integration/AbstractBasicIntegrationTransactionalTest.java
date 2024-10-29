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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.transactions.Transaction;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractBasicIntegrationTransactionalTest extends AbstractBasicIntegrationTest {
    /** */
    @Parameterized.Parameter()
    public SqlTransactionMode sqlTxMode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "sqlTxMode={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(SqlTransactionMode.values());
    }

    /** */
    protected static SqlTransactionMode currentMode;

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
    protected void init() throws Exception {
        startGrids(nodeCount());

        client = startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected QueryChecker assertQuery(IgniteEx ignite, String qry) {
        return new QueryChecker(qry, tx, sqlTxMode) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(ignite.context(), QueryEngine.class);
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> executeSql(IgniteEx ignite, String sql, Object... args) {
        if (sqlTxMode != SqlTransactionMode.NONE && tx == null)
            startTransaction(ignite);

        return super.executeSql(ignite, sql, args);
    }

    /** {@inheritDoc} */
    @Override protected QueryContext queryContext() {
        return QueryContext.of(tx != null ? ((TransactionProxyImpl<?, ?>)tx).tx().xidVersion() : null);
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> sql(IgniteEx ignite, String sql, Object... params) {
        if (sqlTxMode != SqlTransactionMode.NONE && tx == null)
            startTransaction(ignite);

        return super.sql(ignite, sql, params);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> void put(Ignite node, IgniteCache<K, V> cache, K key, V val) {
        invokeAction(node, () -> {
            cache.put(key, val);
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration() {
        return super.<K, V>cacheConfiguration().setAtomicityMode(sqlTxMode == SqlTransactionMode.NONE
            ? CacheAtomicityMode.ATOMIC
            : CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    protected <T> T invokeAction(Ignite node, SupplierX<T> action) {
        if (tx == null && sqlTxMode != SqlTransactionMode.NONE)
            startTransaction(node);

        switch (sqlTxMode) {
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
    protected void clearTransaction() {
        if (tx == null)
            return;

        tx.resume();

        tx.rollback();

        tx = null;
    }

    /** */
    public String atomicity() {
        return "atomicity=" + (sqlTxMode == SqlTransactionMode.NONE ? CacheAtomicityMode.ATOMIC : CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    public interface SupplierX<T> {
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
    public enum SqlTransactionMode {
        /** All put, remove and SQL dml will be executed inside transaction. */
        ALL,

        /** Only some DML operations will be executed inside transaction. */
        RANDOM,

        /** Don't use transaction for DML. */
        NONE
    }
}
