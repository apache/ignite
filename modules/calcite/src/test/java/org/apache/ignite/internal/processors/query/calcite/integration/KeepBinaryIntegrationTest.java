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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.SupplierX;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Test "keep binary" in cache queries.
 */
public class KeepBinaryIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());
        cfg.setCacheConfiguration(cacheConfiguration().setName(CACHE_NAME).setIndexedTypes(Integer.class, Person.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean destroyCachesAfterTest() {
        return false;
    }

    /** */
    @Test
    public void testKeepBinary() {
        IgniteCache<Integer, Person> cache = client.cache(CACHE_NAME);

        Person p0 = new Person(0, "name0", null);
        Person p1 = new Person(1, "name1", new Person(2, "name2",
            new Person(3, "name3", null)));

        Person p2 = new Person(2, "name2", F.asList(new Person(3, "name3", null),
            new Person(4, "name4", null)));

        put(client, cache, 0, p0);
        put(client, cache, 1, p1);
        put(client, cache, 2, p2);

        SupplierX<?> checker = () -> {
            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT _VAL, obj, name FROM Person ORDER BY id")).getAll();

            assertEquals(3, res.size());
            assertEquals(p0, res.get(0).get(0));
            assertEquals(p1, res.get(1).get(0));
            assertEquals(p2, res.get(2).get(0));
            assertEquals(p0.obj, res.get(0).get(1));
            assertEquals(p1.obj, res.get(1).get(1));
            assertEquals(p2.obj, res.get(2).get(1));
            assertEquals(p0.name, res.get(0).get(2));
            assertEquals(p1.name, res.get(1).get(2));
            assertEquals(p2.name, res.get(2).get(2));

            res = cache.withKeepBinary().query(new SqlFieldsQuery("SELECT _VAL, obj, name FROM Person ORDER BY id")).getAll();

            assertEquals(3, res.size());
            assertTrue(res.get(0).get(0) instanceof BinaryObject);
            assertTrue(res.get(1).get(0) instanceof BinaryObject);
            assertTrue(res.get(1).get(0) instanceof BinaryObject);
            assertNull(res.get(0).get(1));
            assertTrue(res.get(1).get(1) instanceof BinaryObject);
            assertTrue(res.get(1).get(1) instanceof BinaryObject);
            assertEquals(p0.name, res.get(0).get(2));
            assertEquals(p1.name, res.get(1).get(2));
            assertEquals(p2.name, res.get(2).get(2));

            return null;
        };

        if (sqlTxMode == SqlTransactionMode.NONE)
            checker.get();
        else
            txAction(client, checker);
    }

    /** */
    @Test
    public void testDynamicParameters() {
        IgniteCache<Integer, Person> cache = client.cache(CACHE_NAME);

        Person p0 = new Person(0, "name0", null);
        Person p1 = new Person(1, "name1", p0);

        put(client, cache, 0, p0);
        put(client, cache, 1, p1);

        SupplierX<?> checker = () -> {
            for (boolean keepBinary : new boolean[] {true, false}) {
                for (String sql : new String[] {
                    "SELECT ?",
                    "SELECT _val FROM Person WHERE _val = ?",
                    "SELECT obj FROM Person WHERE obj = ?"
                }) {
                    SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs(p0);

                    List<List<?>> res = keepBinary ? cache.withKeepBinary().query(qry).getAll() : cache.query(qry).getAll();

                    assertEquals(1, res.size());

                    if (keepBinary)
                        assertTrue(res.get(0).get(0) instanceof BinaryObject);
                    else
                        assertEquals(p0, res.get(0).get(0));
                }

                SqlFieldsQuery qry = new SqlFieldsQuery("SELECT ?").setArgs(F.asList(p0));

                List<List<?>> res = keepBinary ? cache.withKeepBinary().query(qry).getAll() : cache.query(qry).getAll();

                assertEquals(1, res.size());

                if (keepBinary) {
                    assertTrue(res.get(0).get(0) instanceof List);
                    assertTrue(((List<?>)res.get(0).get(0)).get(0) instanceof BinaryObject);
                }
                else
                    assertEquals(F.asList(p0), res.get(0).get(0));
            }

            return null;
        };

        if (sqlTxMode == SqlTransactionMode.NONE)
            checker.get();
        else
            txAction(client, checker);
    }

    /** */
    @Test
    public void testSimpleInsert() {
        IgniteCache<Object, Object> cache = null;

        try {
            cache = client.createCache("testInsert");

            runQuery(0, nodeCount() * 10, false, cache);

            cache = cache.withKeepBinary();

            runQuery(nodeCount() * 10, 2 * nodeCount() * 10, false, cache);

            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM emp")).getAll();

            assertEquals("Unexpected result set size: " + res.size(), 1, res.size());
        }
        finally {
            if (cache != null)
                cache.destroy();

            sql("DROP TABLE IF EXISTS emp");
        }
    }

    /** */
    @Test
    public void testTxInsert() {
        IgniteCache<?, ?> cache = null;

        try {
            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("testInsert");
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            cache = client.createCache(ccfg);

            runQuery(0, nodeCount() * 10, true, cache);

            cache = cache.withKeepBinary();

            runQuery(nodeCount() * 10, 2 * nodeCount() * 10, true, cache);

            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM emp")).getAll();

            assertEquals("Unexpected result set size: " + res.size(), 1, res.size());
        }
        finally {
            if (cache != null)
                cache.destroy();

            sql("DROP TABLE IF EXISTS emp");
        }
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

    /** */
    private static class Person {
        /** */
        @QuerySqlField
        private final int id;

        /** */
        @QuerySqlField
        private final String name;

        /** */
        @QuerySqlField
        private final Object obj;

        /** */
        private Person(int id, String name, Object obj) {
            this.id = id;
            this.name = name;
            this.obj = obj;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;
            return id == person.id && name.equals(person.name) && Objects.equals(obj, person.obj);
        }
    }
}
