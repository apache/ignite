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

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Public api integration tests. */
public class PublicApiIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)));

        return cfg;
    }

    /** */
    @Test
    public void testSimpleInsert() {
        IgniteCache<Object, Object> cache = client.createCache(DEFAULT_CACHE_NAME);

        runQuery(0, nodeCount() * 10, false, cache);

        cache = cache.withKeepBinary();

        runQuery(nodeCount() * 10, 2 * nodeCount() * 10, false, cache);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM emp")).getAll();

        assertEquals("Unexpected result set size: " + res.size(), 1, res.size());
    }

    /** */
    @Test
    public void testTxInsert() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<?, ?> cache = client.createCache(ccfg);

        runQuery(0, nodeCount() * 10, true, cache);

        cache = cache.withKeepBinary();

        runQuery(nodeCount() * 10, 2 * nodeCount() * 10, true, cache);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM emp")).getAll();

        assertEquals("Unexpected result set size: " + res.size(), 1, res.size());
    }

    /** */
    private void runQuery(int begin, int end, boolean transactional, IgniteCache<?, ?> cache) {
        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS emp(empid INTEGER, deptid INTEGER, name VARCHAR, salary INTEGER, " +
            "PRIMARY KEY(empid, deptid)) WITH \"AFFINITY_KEY=deptid" + (transactional ? ", ATOMICITY=transactional" : "") + "\""))
            .getAll();

        try (Transaction tx = transactional ? client.transactions().txStart(PESSIMISTIC, READ_COMMITTED) : null) {
            for (int i = begin; i < end; i++) {
                cache.query(new SqlFieldsQuery("INSERT INTO emp (empid, deptid, name, salary) VALUES (?, ?, ?, ?)").setArgs(
                    i, i % 2, "Employee " + i, i)).getAll();

                cache.query(new SqlFieldsQuery("UPDATE emp SET name = '' WHERE empid = ? AND deptid = ?").setArgs(i, i % 2)).getAll();
                cache.query(new SqlFieldsQuery("DELETE FROM emp WHERE empid = ?").setArgs(i - 1)).getAll();

                cache.query(new SqlFieldsQuery(
                    "MERGE INTO emp dst USING table(system_range(1, 1000)) src ON dst.salary = src.x " +
                        "WHEN MATCHED THEN UPDATE SET dst.salary = src.x")).getAll();
            }

            if (tx != null)
                tx.commit();
        }
    }
}
