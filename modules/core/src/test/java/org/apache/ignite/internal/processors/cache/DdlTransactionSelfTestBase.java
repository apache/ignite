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

import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.QueryEngineConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 *
 */
public abstract class DdlTransactionSelfTestBase extends GridCommonAbstractTest {
    /** Create table request. */
    public static final String CREATE_TABLE = "CREATE TABLE " +
        "person (id int, name varchar, age int, company varchar, city varchar, primary key (id, name, city))" +
        "WITH " +
        "\"cache_name=test_cache,template=PARTITIONED,atomicity=TRANSACTIONAL,affinity_key=city\"";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTransactionConfiguration(new TransactionConfiguration()
            .setDefaultTxIsolation(TransactionIsolation.REPEATABLE_READ)
            .setDefaultTxConcurrency(TransactionConcurrency.PESSIMISTIC)
            .setDefaultTxTimeout(5000));

        cfg.setCacheConfiguration(getCacheConfiguration());

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(getQueryEngineConfiguration()));

        return cfg;
    }

    /** */
    protected abstract QueryEngineConfiguration getQueryEngineConfiguration();

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> getCacheConfiguration() {
        return defaultCacheConfiguration()
            .setBackups(1)
            .setNearConfiguration(null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestFailsInsideTxMultinodeClient() throws Exception {
        startGridsMultiThreaded(4, false);

        Ignite node = startClientGrid(4);

        awaitPartitionMapExchange();

        doTestDdlFailsInsideTx(node);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestFailsInsideTxMultinode() throws Exception {
        Ignite node = startGridsMultiThreaded(4);

        doTestDdlFailsInsideTx(node);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestFailsInsideTx() throws Exception {
        Ignite node = startGrid();

        doTestDdlFailsInsideTx(node);
    }

    /** */
    private void doTestDdlFailsInsideTx(Ignite node) {
        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = node.transactions().txStart()) {
            cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

            String errMsg = "Cannot start/stop cache within lock or transaction [cacheNames=test_cache, " +
                "operation=dynamicStartCache]";

            Throwable cacheEx = assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery(CREATE_TABLE)
                        .setSchema("PUBLIC"))
                    .getAll(),
                CacheException.class,
                errMsg);

            assertTrue(X.hasCause(cacheEx, errMsg, IgniteException.class));

            assertTrue(tx.state() != TransactionState.COMMITTED);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestWithoutTxMultinodeClient() throws Exception {
        startGridsMultiThreaded(4, false);

        Ignite node = startClientGrid(4);

        awaitPartitionMapExchange();

        doTestDdlWithoutTx(node);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestWithoutTxMultinode() throws Exception {
        Ignite node = startGridsMultiThreaded(4);

        doTestDdlWithoutTx(node);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestWithoutTx() throws Exception {
        Ignite node = startGrid();

        doTestDdlWithoutTx(node);
    }

    /** */
    private void doTestDdlWithoutTx(Ignite node) {
        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(CREATE_TABLE).setSchema("PUBLIC"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("SELECT * FROM person")
            .setSchema("PUBLIC"))
        ) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }
}
