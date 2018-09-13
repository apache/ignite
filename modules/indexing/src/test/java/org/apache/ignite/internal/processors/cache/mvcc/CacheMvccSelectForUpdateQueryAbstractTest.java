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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.CacheException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;

/**
 * Test for {@code SELECT FOR UPDATE} queries.
 */
public abstract class CacheMvccSelectForUpdateQueryAbstractTest extends CacheMvccAbstractTest {
    /** */
    private static final int CACHE_SIZE = 50;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        disableScheduledVacuum = getName().equals("testSelectForUpdateAfterAbortedTx");

        startGrids(3);

        CacheConfiguration seg = new CacheConfiguration("segmented*");

        seg.setCacheMode(cacheMode());

        if (seg.getCacheMode() == PARTITIONED)
            seg.setQueryParallelism(4);

        grid(0).addCacheConfiguration(seg);

        Thread.sleep(1000L);

        try (Connection c = connect(grid(0))) {
            execute(c, "create table person (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional,cache_name=Person\"");

            execute(c, "create table person_seg (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional,cache_name=PersonSeg,template=segmented\"");

            try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ)) {

                for (int i = 1; i <= CACHE_SIZE; i++) {
                    execute(c, "insert into person(id, firstName, lastName) values(" + i + ",'" + i + "','" + i + "')");

                    execute(c, "insert into person_seg(id, firstName, lastName) " +
                        "values(" + i + ",'" + i + "','" + i + "')");
                }

                tx.commit();
            }
        }
    }

    /**
     *
     */
    public void testSelectForUpdateDistributed() throws Exception {
        doTestSelectForUpdateDistributed("Person", false);
    }


    /**
     *
     */
    public void testSelectForUpdateLocal() throws Exception {
        doTestSelectForUpdateLocal("Person", false);
    }

    /**
     *
     * @throws Exception If failed.
     */
    public void testSelectForUpdateOutsideTx() throws Exception {
        doTestSelectForUpdateDistributed("Person", true);
    }

    /**
     *
     * @throws Exception If failed.
     */
    public void testSelectForUpdateOutsideTxLocal() throws Exception {
        doTestSelectForUpdateLocal("Person", true);
    }

    /**
     * @param cacheName Cache name.
     * @param outsideTx Whether select is executed outside transaction
     * @throws Exception If failed.
     */
    void doTestSelectForUpdateLocal(String cacheName, boolean outsideTx) throws Exception {
        Ignite node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache(cacheName);

        Transaction ignored = outsideTx ? null : node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ);

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery("select id, * from " + tableName(cache) + " order by id for update")
                .setLocal(true);

            FieldsQueryCursor<List<?>> query = cache.query(qry);

            List<List<?>> res = query.getAll();

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res)
                keys.add((Integer)r.get(0));

            checkLocks(cacheName, keys, !outsideTx);
        }
        finally {
            U.close(ignored, log);
        }
    }

    /**
     * @param cacheName Cache name.
     * @param outsideTx Whether select is executed outside transaction
     * @throws Exception If failed.
     */
    void doTestSelectForUpdateDistributed(String cacheName, boolean outsideTx) throws Exception {
        Ignite node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache(cacheName);

        Transaction ignored = outsideTx ? null : node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ);

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery("select id, * from " + tableName(cache) + " order by id for update")
                .setPageSize(10);

            FieldsQueryCursor<List<?>> query = cache.query(qry);

            List<List<?>> res = query.getAll();

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res)
                keys.add((Integer)r.get(0));

            checkLocks(cacheName, keys, !outsideTx);
        }
        finally {
            U.close(ignored, log);
        }
    }

    /**
     *
     */
    public void testSelectForUpdateWithUnion() {
        assertQueryThrows("select id from person union select 1 for update",
            "SELECT UNION FOR UPDATE is not supported.");
    }

    /**
     *
     */
    public void testSelectForUpdateWithJoin() {
        assertQueryThrows("select p1.id from person p1 join person p2 on p1.id = p2.id for update",
            "SELECT FOR UPDATE with joins is not supported.");
    }

    /**
     *
     */
    public void testSelectForUpdateWithLimit() {
        assertQueryThrows("select id from person limit 0,5 for update",
            "LIMIT/OFFSET clauses are not supported for SELECT FOR UPDATE.");
    }

    /**
     *
     */
    public void testSelectForUpdateWithGroupings() {
        assertQueryThrows("select count(*) from person for update",
            "SELECT FOR UPDATE with aggregates and/or GROUP BY is not supported.");

        assertQueryThrows("select lastName, count(*) from person group by lastName for update",
            "SELECT FOR UPDATE with aggregates and/or GROUP BY is not supported.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSelectForUpdateAfterAbortedTx() throws Exception {
        assert disableScheduledVacuum;

        Ignite node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache("Person");

        List<List<?>> res;

        try (Transaction tx = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ)) {

            res = cache.query(new SqlFieldsQuery("update person set lastName=UPPER(lastName)")).getAll();

            assertEquals((long)CACHE_SIZE, res.get(0).get(0));

            tx.rollback();
        }

        try (Transaction tx = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ)) {

            res = cache.query(new SqlFieldsQuery("select id, * from person order by id for update")).getAll();

            assertEquals(CACHE_SIZE, res.size());

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res)
                keys.add((Integer)r.get(0));

            checkLocks("Person", keys, true);

            tx.rollback();
        }
    }

    /**
     * Check that an attempt to get a lock on any key from given list fails by timeout.
     *
     * @param cacheName Cache name to check.
     * @param keys Keys to check.
     * @param locked Whether the key is locked
     * @throws Exception if failed.
     */
    @SuppressWarnings({"ThrowableNotThrown", "unchecked"})
    private void checkLocks(String cacheName, List<Integer> keys, boolean locked) throws Exception {
        Ignite node = ignite(2);
        IgniteCache cache = node.cache(cacheName);

        List<IgniteInternalFuture<Integer>> calls = new ArrayList<>();

        for (int key : keys) {
            calls.add(GridTestUtils.runAsync(new Callable<Integer>() {
                /** {@inheritDoc} */
                @Override public Integer call() {
                    try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ)) {
                        List<List<?>> res = cache
                            .query(
                                new SqlFieldsQuery("select * from " + tableName(cache) +
                                    " where id = " + key + " for update").setTimeout(1, TimeUnit.SECONDS)
                            )
                            .getAll();

                        return (Integer)res.get(0).get(0);
                    }
                }
            }));
        }

        for (IgniteInternalFuture fut : calls) {
            if (!locked)
                fut.get(TX_TIMEOUT);
            else {
                try {
                    fut.get();
                }
                catch (Exception e) {
                    CacheException e0 = X.cause(e, CacheException.class);

                    assert e0 != null;

                    assert e0.getMessage() != null &&
                        e0.getMessage().contains("Failed to acquire lock within provided timeout");
                }
            }
        }
    }

    /**
     * @param cache Cache.
     * @return Name of the table contained by this cache.
     */
    @SuppressWarnings("unchecked")
    private static String tableName(IgniteCache<?, ?> cache) {
        return ((Collection<QueryEntity>)cache.getConfiguration(CacheConfiguration.class).getQueryEntities())
            .iterator().next().getTableName();
    }

    /**
     * Test that query throws exception with expected message.
     * @param qry SQL.
     * @param exMsg Expected message.
     */
    private void assertQueryThrows(String qry, String exMsg) {
        assertQueryThrows(qry, exMsg, false);

        assertQueryThrows(qry, exMsg, true);
    }

    /**
     * Test that query throws exception with expected message.
     * @param qry SQL.
     * @param exMsg Expected message.
     * @param loc Local query flag.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertQueryThrows(String qry, String exMsg, boolean loc) {
        Ignite node = grid(0);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() {
                return node.cache("Person").query(new SqlFieldsQuery(qry).setLocal(loc)).getAll();
            }
        }, IgniteSQLException.class, exMsg);
    }
}
