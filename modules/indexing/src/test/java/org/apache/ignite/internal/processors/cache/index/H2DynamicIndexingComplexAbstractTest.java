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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Base class for testing work of combinations of DML and DDL operations.
 */
public abstract class H2DynamicIndexingComplexAbstractTest extends DynamicIndexAbstractSelfTest {
    /** Cache mode to test with. */
    private final CacheMode cacheMode;

    /** Cache atomicity mode to test with. */
    private final CacheAtomicityMode atomicityMode;

    /** Node index to initiate operations from. */
    private final int nodeIdx;

    /** Backups to configure */
    private final int backups;

    /** Names of companies to use. */
    private final static List<String> COMPANIES = Arrays.asList("ASF", "GNU", "BSD");

    /** Cities to use. */
    private final static List<String> CITIES = Arrays.asList("St. Petersburg", "Boston", "Berkeley", "London");

    /** Index of server node. */
    protected final static int SRV_IDX = 0;

    /** Index of client node. */
    protected final static int CLIENT_IDX = 1;

    /**
     * Constructor.
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @param backups Number of backups.
     * @param nodeIdx Node index.
     */
    H2DynamicIndexingComplexAbstractTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode, int backups, int nodeIdx) {
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
        this.backups = backups;
        this.nodeIdx = nodeIdx;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignition.start(serverConfiguration(0));

        Ignition.start(clientConfiguration(1));

        Ignition.start(serverConfiguration(2));

        Ignition.start(serverConfiguration(3));
    }

    /** Do test. */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testOperations() {
        executeSql("CREATE TABLE person (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id, name, city)) WITH \"template=" + cacheMode.name() + ",atomicity=" + atomicityMode.name() +
            ",backups=" + backups + ",affinity_key=city\"");

        executeSql("CREATE INDEX idx on person (city asc, name asc)");

        executeSql("CREATE TABLE city (name varchar, population int, primary key (name)) WITH " +
            "\"template=" + cacheMode.name() + ",atomicity=" + atomicityMode.name() +
            ",backups=" + backups + ",affinity_key=name\"");

        executeSql("INSERT INTO city (name, population) values(?, ?), (?, ?), (?, ?)",
            "St. Petersburg", 6000000,
            "Boston", 2000000,
            "London", 8000000
        );

        final long PERSON_COUNT = 100;

        for (int i = 0; i < PERSON_COUNT; i++)
            executeSql("INSERT INTO person (id, name, age, company, city) values (?, ?, ?, ?, ?)",
                i,
                "Person " + i,
                20 + (i % 10),
                COMPANIES.get(i % COMPANIES.size()),
                CITIES.get(i % CITIES.size()));

        assertAllPersons(new IgniteInClosure<List<?>>() {
            @Override public void apply(List<?> person) {
                assertInitPerson(person);
            }
        });

        long r = (Long)executeSqlSingle("SELECT COUNT(*) from Person");

        assertEquals(PERSON_COUNT, r);

        r = (Long)executeSqlSingle("SELECT COUNT(*) from Person p inner join City c on p.city = c.name");

        // Berkeley is not present in City table, although 25 people have it specified as their city.
        assertEquals(75L, r);

        executeSqlSingle("UPDATE Person SET company = 'GNU', age = CASE WHEN MOD(id, 2) <> 0 THEN age + 5 ELSE " +
            "age + 1 END WHERE company = 'ASF'");

        assertAllPersons(new IgniteInClosure<List<?>>() {
            @Override public void apply(List<?> person) {
                int id = (Integer)person.get(0);

                if (id % COMPANIES.size() == 0) {
                    int initAge = 20 + id % 10;

                    int expAge = (initAge % 2 != 0 ? initAge + 5 : initAge + 1);

                    assertPerson(id, "Person " + id, expAge, "GNU", CITIES.get(id % CITIES.size()), person);
                }
                else
                    assertInitPerson(person);
            }
        });

        executeSql("DROP INDEX idx");

        // Index drop should not affect data.
        assertAllPersons(new IgniteInClosure<List<?>>() {
            @Override public void apply(List<?> person) {
                int id = (Integer)person.get(0);

                if (id % COMPANIES.size() == 0) {
                    int initAge = 20 + id % 10;

                    int expAge = initAge % 2 != 0 ? initAge + 5 : initAge + 1;

                    assertPerson(id, "Person " + id, expAge, "GNU", CITIES.get(id % CITIES.size()), person);
                }
                else
                    assertInitPerson(person);
            }
        });

        // Let's drop all BSD folks living in Berkeley and Boston - this compares ASCII codes of 1st symbols.
        executeSql("DELETE FROM person WHERE ASCII(company) = ASCII(city)");

        assertAllPersons(new IgniteInClosure<List<?>>() {
            @Override public void apply(List<?> person) {
                String city = city(person);

                String company = company(person);

                assertFalse(city.charAt(0) == company.charAt(0));
            }
        });

        assertNotNull(node().cache("SQL_PUBLIC_PERSON"));

        executeSql("DROP TABLE person");

        assertNull(node().cache("SQL_PUBLIC_PERSON"));

        GridTestUtils.assertThrows(null, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("SELECT * from Person");
            }
        }, IgniteSQLException.class, "Table \"PERSON\" not found");
    }

    /**
     * Select all data from the table and check that all records correspond to rules set by given closure.
     * @param clo Closure to apply to each record.
     */
    private void assertAllPersons(IgniteInClosure<List<?>> clo) {
        List<List<?>> res = executeSql("SELECT * from Person");

        for (List<?> p : res)
            clo.apply(p);
    }

    /**
     * Get person's id from data row.
     * @param person data row.
     * @return person's id.
     */
    private static int id(List<?> person) {
        return (Integer)person.get(0);
    }

    /**
     * Get person's name from data row.
     * @param person data row.
     * @return person's name.
     */
    private static String name(List<?> person) {
        return (String)person.get(1);
    }

    /**
     * Get person's age from data row.
     * @param person data row.
     * @return person's age.
     */
    private static int age(List<?> person) {
        return (Integer)person.get(2);
    }

    /**
     * Get person's company from data row.
     * @param person data row.
     * @return person's company.
     */
    private static String company(List<?> person) {
        return (String)person.get(3);
    }

    /**
     * Get person's city from data row.
     * @param person data row.
     * @return person's city.
     */
    private static String city(List<?> person) {
        return (String)person.get(4);
    }

    /**
     * Check that all columns in data row are exactly how they were initially inserted.
     * @param person data row.
     */
    private void assertInitPerson(List<?> person) {
        assertEquals(5, person.size());

        int id = id(person);

        String name = "Person " + id;

        int age = 20 + id % 10;

        String company = COMPANIES.get(id % COMPANIES.size());

        String city = CITIES.get(id % CITIES.size());

        assertPerson(id, name, age, company, city, person);
    }

    /**
     * Check contents of SQL data row and corresponding cache entry.
     * @param id Expected id.
     * @param name Expected name.
     * @param age Expected age.
     * @param company Expected company.
     * @param city Expected city.
     * @param person Data row.
     */
    private void assertPerson(int id, String name, int age, String company, String city, List<?> person) {
        assertEquals(name, name(person));

        assertEquals(age, age(person));

        assertEquals(company, company(person));

        assertEquals(city, city(person));

        String cacheName = "SQL_PUBLIC_PERSON";

        Collection<GridQueryTypeDescriptor> descs = node().context().query().types(cacheName);

        assertEquals(1, descs.size());

        GridQueryTypeDescriptor desc = descs.iterator().next();

        String keyType = desc.keyTypeName();

        String valType = desc.valueTypeName();

        BinaryObject k = node().binary().builder(keyType)
            .setField("id", id)
            .setField("name", name)
            .setField("city", city).build();

        Object v = node().cache(cacheName).withKeepBinary().get(k);

        assertNotNull(v);

        BinaryObject expVal = node().binary().builder(valType)
            .setField("age", age)
            .setField("company", company).build();

        assertEquals(expVal, v);
    }

    /**
     * Run SQL statement on specified node.
     * @param stmt Statement to run.
     * @return Run result.
     */
    private List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /**
     * Run SQL statement on default node.
     * @param stmt Statement to run.
     * @return Run result.
     */
    private List<List<?>> executeSql(String stmt, Object... args) {
        return executeSql(node(), stmt, args);
    }

    /**
     * Run SQL statement that is expected to return strictly one value (like COUNT(*)).
     * @param stmt Statement to run.
     * @return Run result.
     */
    private Object executeSqlSingle(String stmt, Object... args) {
        List<List<?>> res = executeSql(stmt, args);

        assertEquals(1, res.size());

        List<?> row = res.get(0);

        assertEquals(1, row.size());

        return row.get(0);
    }

    /**
     * @return Node to initiate operations from.
     */
    protected IgniteEx node() {
        return grid(nodeIdx);
    }
}
