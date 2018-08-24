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

package org.apache.ignite.internal.processors.cache.transaction;

import java.util.Properties;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests DML allow/disallow operation inside transaction.
 */
public class DmlInsideTransactionTest extends GridCommonAbstractTest {

    /** Person cache name. */
    private static final String CACHE_PERSON = "PersonCache";

    /** Set of DML queries for tests */
    private final static String[] DML_QUERIES = {
        "MERGE INTO TEST.Person(id, name, orgId) VALUES(111,'NAME',111)",
        "INSERT INTO TEST.Person(id, name, orgId) VALUES(222,'NAME',111)",
        "UPDATE TEST.Person SET name='new name'",
        "DELETE TEST.Person WHERE id=1",
        "INSERT INTO TEST.Person(id, name, orgId) SELECT id+1000, name, orgId FROM TEST.Person"
    };

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checking correct behaviour for DML inside transaction by default.
     *
     * @throws Exception In case failure.
     */
    public void testDmlInTransactionByDefault() throws Exception {
        prepareIgnite();

        for (String dmlQuery : DML_QUERIES)
            runDmlInTransactionTest(dmlQuery, false);
    }

    /**
     * Checking correct behaviour for DML inside transaction when compatibility property set as disabled.
     *
     * @throws Exception In case failure.
     */
    public void testDmlInTransactionInDisabledCompatibilityMode() throws Exception {
        try (SystemProperty ignored = new SystemProperty(IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION, "false")) {
            prepareIgnite();

            for (String dmlQuery : DML_QUERIES)
                runDmlInTransactionTest(dmlQuery, false);
        }
    }

    /**
     * Checking correct behaviour for DML inside transaction when compatibility property set as enabled.
     *
     * @throws Exception In case failure.
     */
    public void testDmlInTransactionInCompatibilityMode() throws Exception {
        try (SystemProperty ignored = new SystemProperty(IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION, "true")) {
            prepareIgnite();

            for (String dmlQuery : DML_QUERIES)
                runDmlInTransactionTest(dmlQuery, true);
        }
    }

    /**
     * Checking that DML can be executed without a errors outside transaction.
     *
     * @throws Exception In case failure.
     */
    public void testDmlNotInTransaction() throws Exception {
        prepareIgnite();

        for (String dmlQuery : DML_QUERIES)
            grid(0).cache(CACHE_PERSON).query(new SqlFieldsQuery(dmlQuery));
    }

    /**
     * Start Ignite grid and create cache.
     *
     * @throws Exception In case is failure.
     */
    private void prepareIgnite() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.createCache(new CacheConfiguration<PersonKey, Person>()
            .setName(CACHE_PERSON)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setSqlSchema("TEST")
            .setIndexedTypes(PersonKey.class, Person.class));
    }

    /**
     * Run DML query and check that DML is not allowed or not inside transaction. Also checked that using DML will not
     * lead to rollback
     *
     * @param dmlQry Dml query which should be executed in transaction.
     * @param isAllowed true in case DML should work inside transaction, false otherwise.
     */
    private void runDmlInTransactionTest(String dmlQry, boolean isAllowed) {
        IgniteEx ignite = grid(0);

        IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_PERSON);

        cache.removeAll();

        assertEquals(0, cache.query(new SqlFieldsQuery("SELECT * FROM TEST.Person")).getAll().size());

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(new PersonKey(1L), new Person("person", 2));

            if (isAllowed)
                cache.query(new SqlFieldsQuery(dmlQry));
            else {
                assertThrows(log, () -> {
                    cache.query(new SqlFieldsQuery(dmlQry).setLocal(true));

                    return null;
                }, CacheException.class, "DML statement doesn't allowed within a transaction");
            }

            tx.commit();
        }

        assertTrue(cache.query(new SqlFieldsQuery("SELECT * FROM TEST.Person")).getAll().size() > 0);
    }

    /**
     * Person key.
     */
    public static class PersonKey {
        /** ID. */
        @QuerySqlField
        public long id;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        PersonKey(long id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof PersonKey && (F.eq(id, ((PersonKey)obj).id));
        }
    }

    /**
     * Person.
     */
    public static class Person {
        /** Name. */
        @QuerySqlField
        public String name;

        /** Organization ID. */
        @QuerySqlField(index = true)
        public long orgId;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param orgId Orgainzation ID.
         */
        public Person(String name, long orgId) {
            this.name = name;
            this.orgId = orgId;
        }
    }

    /** Adds system property on initialization and removes it when closed. */
    private static final class SystemProperty implements AutoCloseable {
        /** Name. */
        private final String name;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param val Value.
         */
        SystemProperty(String name, String val) {
            this.name = name;

            Properties props = System.getProperties();
            props.put(name, val);

            System.setProperties(props);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            Properties props = System.getProperties();
            props.remove(name);

            System.setProperties(props);
        }
    }
}
