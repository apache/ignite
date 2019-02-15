/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class DdlTransactionSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

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
        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration getCacheConfiguration() {
        CacheConfiguration<?, ?> ccfg = defaultCacheConfiguration();

        ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxIsCommittedOnDdlRequestMultinodeClient() throws Exception {
        startGridsMultiThreaded(4, false);

        client = true;

        Ignite node = startGrid(4);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = node.transactions().txStart()) {
            cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

            try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
                "CREATE TABLE " +
                    "    person (id int, name varchar, age int, company varchar, city varchar, primary key (id, name, city))" +
                    "WITH " +
                    "    \"template=PARTITIONED,atomicity=TRANSACTIONAL,affinity_key=city\"").setSchema("PUBLIC"))) {

                assertNotNull(cur);

                List<List<?>> rows = cur.getAll();

                assertEquals(1, rows.size());

                assertEquals(0L, rows.get(0).get(0));
            }

            assertTrue(tx.state() == TransactionState.COMMITTED);
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("SELECT * FROM person").setSchema("PUBLIC"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxIsCommittedOnDdlRequestMultinode() throws Exception {
        Ignite node = startGridsMultiThreaded(4);

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = node.transactions().txStart()) {
            cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

            try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
                "CREATE TABLE " +
                    "    person (id int, name varchar, age int, company varchar, city varchar, primary key (id, name, city))" +
                    "WITH " +
                    "    \"template=PARTITIONED,atomicity=TRANSACTIONAL,affinity_key=city\"").setSchema("PUBLIC"))) {

                assertNotNull(cur);

                List<List<?>> rows = cur.getAll();

                assertEquals(1, rows.size());

                assertEquals(0L, rows.get(0).get(0));
            }

            assertTrue(tx.state() == TransactionState.COMMITTED);
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("SELECT * FROM person").setSchema("PUBLIC"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxIsCommittedOnDdlRequest() throws Exception {
        Ignite node = startGrid();

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = node.transactions().txStart()) {
            cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

            try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
                "CREATE TABLE " +
                    "    person (id int, name varchar, age int, company varchar, city varchar, primary key (id, name, city))" +
                    "WITH " +
                    "    \"template=PARTITIONED,atomicity=TRANSACTIONAL,affinity_key=city\"").setSchema("PUBLIC"))) {

                assertNotNull(cur);

                List<List<?>> rows = cur.getAll();

                assertEquals(1, rows.size());

                assertEquals(0L, rows.get(0).get(0));
            }

            assertTrue(tx.state() == TransactionState.COMMITTED);
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("SELECT * FROM person").setSchema("PUBLIC"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestWithoutTxMultinodeClient() throws Exception {
        startGridsMultiThreaded(4, false);

        client = true;

        Ignite node = startGrid(4);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "CREATE TABLE " +
                "    person (id int, name varchar, age int, company varchar, city varchar, primary key (id, name, city))" +
                "WITH " +
                "    \"template=PARTITIONED,atomicity=TRANSACTIONAL,affinity_key=city\"").setSchema("PUBLIC"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("SELECT * FROM person").setSchema("PUBLIC"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestWithoutTxMultinode() throws Exception {
        Ignite node = startGridsMultiThreaded(4);

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "CREATE TABLE " +
                "    person (id int, name varchar, age int, company varchar, city varchar, primary key (id, name, city))" +
                "WITH " +
                "    \"template=PARTITIONED,atomicity=TRANSACTIONAL,affinity_key=city\"").setSchema("PUBLIC"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("SELECT * FROM person").setSchema("PUBLIC"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDdlRequestWithoutTx() throws Exception {
        Ignite node = startGrid();

        IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(1, 1, 2, 2, 3, 3));

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "CREATE TABLE " +
                "    person (id int, name varchar, age int, company varchar, city varchar, primary key (id, name, city))" +
                "WITH " +
                "    \"template=PARTITIONED,atomicity=TRANSACTIONAL,affinity_key=city\"").setSchema("PUBLIC"))) {

            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("SELECT * FROM person").setSchema("PUBLIC"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        assertEquals(1, cache.get(1));
        assertEquals(2, cache.get(2));
        assertEquals(3, cache.get(3));
    }
}
