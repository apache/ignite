/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
public class CacheMvccReplicatedSqlTxQueriesTest extends CacheMvccSqlTxQueriesAbstractTest {
    /** {@inheritDoc} */
    protected CacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ccfgs = null;
        ccfg = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedJoinPartitionedClient() throws Exception {
        checkReplicatedJoinPartitioned(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedJoinPartitionedServer() throws Exception {
        checkReplicatedJoinPartitioned(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void checkReplicatedJoinPartitioned(boolean client) throws Exception {
        ccfgs = new CacheConfiguration[] {
            cacheConfiguration(REPLICATED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
                .setName("int")
                .setIndexedTypes(Integer.class, Integer.class),
            cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
                .setIndexedTypes(Integer.class,
                CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class),
            cacheConfiguration(REPLICATED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
                .setName("target")
                .setIndexedTypes(Integer.class, Integer.class)
        };

        startGridsMultiThreaded(3);

        this.client = true;

        startGrid(3);

        Ignite node = client ? grid(3) : grid(0);

        List<List<?>> r;

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            r = runSql(node, "INSERT INTO \"int\".Integer(_key, _val) VALUES (1,1), (2,2), (3,3)");

            assertEquals(3L, r.get(0).get(0));

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            r = runSql(node, "INSERT INTO \"default\".MvccTestSqlIndexValue(_key, idxVal1) " +
                "VALUES (1,10), (2, 20), (3, 30)");

            assertEquals(3L, r.get(0).get(0));

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            r = runSql(node, "INSERT INTO \"target\".Integer(_key, _val) " +
                "SELECT a._key, a.idxVal1*b._val FROM \"default\".MvccTestSqlIndexValue a " +
                "JOIN \"int\".Integer b ON a._key = b._key");

            assertEquals(3L, r.get(0).get(0));

            tx.commit();
        }

        for (int n = 0; n < 3; ++n) {
            node = grid(n);

            r = runSqlLocal(node, "SELECT _key, _val FROM \"target\".Integer ORDER BY _key");

            assertEquals(3L, r.size());

            assertEquals(1, r.get(0).get(0));
            assertEquals(2, r.get(1).get(0));
            assertEquals(3, r.get(2).get(0));

            assertEquals(10, r.get(0).get(1));
            assertEquals(40, r.get(1).get(1));
            assertEquals(90, r.get(2).get(1));
        }
    }

    /**
     *
     * @throws Exception If failed.
     */
    public void testReplicatedAndPartitionedUpdateSingleTransaction() throws Exception {
        ccfgs = new CacheConfiguration[] {
            cacheConfiguration(REPLICATED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
                .setName("rep")
                .setIndexedTypes(Integer.class, Integer.class),
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT)
                .setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class)
                .setName("part"),
        };

        startGridsMultiThreaded(3);

        client = true;

        startGrid(3);

        Random rnd = ThreadLocalRandom.current();

        Ignite node = grid(rnd.nextInt(4));

        List<List<?>> r;

        Cache<Integer, Integer> repCache = node.cache("rep");

        repCache.put(1, 1);
        repCache.put(2, 2);
        repCache.put(3, 3);

        Cache<Integer, MvccTestSqlIndexValue> partCache = node.cache("part");

        partCache.put(1, new MvccTestSqlIndexValue(1));
        partCache.put(2, new MvccTestSqlIndexValue(2));
        partCache.put(3, new MvccTestSqlIndexValue(3));

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TX_TIMEOUT);

            r = runSql(node, "UPDATE \"rep\".Integer SET _val = _key * 10");

            assertEquals(3L, r.get(0).get(0));

            r = runSql(node, "UPDATE  \"part\".MvccTestSqlIndexValue SET idxVal1 = _key * 10");

            assertEquals(3L, r.get(0).get(0));

            tx.commit();
        }

        r = runSql(node, "SELECT COUNT(1) FROM \"rep\".Integer r JOIN \"part\".MvccTestSqlIndexValue p" +
            " ON r._key = p._key WHERE r._val = p.idxVal1");

        assertEquals(3L, r.get(0).get(0));

        for (int n = 0; n < 3; ++n) {
            node = grid(n);

            r = runSqlLocal(node, "SELECT _key, _val FROM \"rep\".Integer ORDER BY _key");

            assertEquals(3L, r.size());

            assertEquals(1, r.get(0).get(0));
            assertEquals(2, r.get(1).get(0));
            assertEquals(3, r.get(2).get(0));

            assertEquals(10, r.get(0).get(1));
            assertEquals(20, r.get(1).get(1));
            assertEquals(30, r.get(2).get(1));
        }
    }

    /**
     * Run query.
     *
     * @param node Node.
     * @param sqlText Query.
     * @return Results.
     */
    private List<List<?>> runSql(Ignite node, String sqlText) {
        GridQueryProcessor qryProc = ((IgniteEx)node).context().query();

        return qryProc.querySqlFields(new SqlFieldsQuery(sqlText), false).getAll();
    }

    /**
     * Run query locally.
     *
     * @param node Node.
     * @param sqlText Query.
     * @return Results.
     */
    private List<List<?>> runSqlLocal(Ignite node, String sqlText) {
        GridQueryProcessor qryProc = ((IgniteEx)node).context().query();

        return qryProc.querySqlFields(new SqlFieldsQuery(sqlText).setLocal(true), false).getAll();
    }
}
