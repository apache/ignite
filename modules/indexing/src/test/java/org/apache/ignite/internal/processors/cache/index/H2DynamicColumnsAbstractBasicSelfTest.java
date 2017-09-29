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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.config.GridTestProperties;

import static org.apache.ignite.testframework.config.GridTestProperties.BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER;

/**
 * Test to check dynamic columns related features.
 */
public abstract class H2DynamicColumnsAbstractBasicSelfTest extends DynamicColumnsAbstractTest {
    /**
     * Index of coordinator node.
     */
    final static int SRV_CRD_IDX = 0;

    /**
     * Index of non coordinator server node.
     */
    final static int SRV_IDX = 1;

    /**
     * Index of client.
     */
    final static int CLI_IDX = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    /**
     * @return Grid configurations to start.
     * @throws Exception if failed.
     */
    private IgniteConfiguration[] configurations() throws Exception {
        return new IgniteConfiguration[] {
            commonConfiguration(0),
            commonConfiguration(1),
            clientConfiguration(2)
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        run(CREATE_SQL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        run(DROP_SQL);

        super.afterTest();
    }

    /**
     * Test column addition to the end of the columns list.
     */
    public void testAddColumnSimple() {
        run("ALTER TABLE Person ADD COLUMN age int");

        doSleep(500);

        QueryField c = c("AGE", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, QueryUtils.DFLT_SCHEMA, "PERSON", c);
    }

    /**
     * Test column addition to the end of the columns list.
     */
    public void testAddFewColumnsSimple() {
        run("ALTER TABLE Person ADD COLUMN (age int, \"city\" varchar)");

        doSleep(500);

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, QueryUtils.DFLT_SCHEMA, "PERSON",
                c("AGE", Integer.class.getName()),
                c("city", String.class.getName()));
    }

    /**
     * Test {@code IF EXISTS} handling.
     */
    public void testIfTableExists() {
        run("ALTER TABLE if exists City ADD COLUMN population int");
    }

    /**
     * Test {@code IF NOT EXISTS} handling.
     */
    public void testIfColumnNotExists() {
        run("ALTER TABLE Person ADD COLUMN if not exists name varchar");
    }

    /**
     * Test {@code IF NOT EXISTS} handling.
     */
    public void testDuplicateColumnName() {
        assertThrows("ALTER TABLE Person ADD COLUMN name varchar", "Column already exists: NAME");
    }

    /**
     * Test behavior in case of missing table.
     */
    public void testMissingTable() {
        assertThrows("ALTER TABLE City ADD COLUMN name varchar", "Table doesn't exist: CITY");
    }

    /** */
    @SuppressWarnings("unchecked")
    public void testComplexOperations() {
        IgniteCache<BinaryObject, BinaryObject> cache = ignite(nodeIndex())
            .cache(QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, "PERSON"));

        run(cache, "ALTER TABLE Person ADD COLUMN city varchar");

        run(cache, "INSERT INTO Person (id, name, city) values (1, 'John Doe', 'New York')");
        run(cache, "INSERT INTO Person (id, name, city) values (2, 'Mike Watts', 'Denver')");
        run(cache, "INSERT INTO Person (id, name, city) values (3, 'Ann Pierce', 'New York')");

        run(cache, "CREATE INDEX pidx1 ON Person(name, city desc)");

        CacheConfiguration<Integer, City> ccfg = defaultCacheConfiguration().setName("City")
            .setIndexedTypes(Integer.class, City.class).setSqlSchema(QueryUtils.DFLT_SCHEMA);

        ccfg.getQueryEntities().iterator().next().setKeyFieldName("id");

        ignite(nodeIndex()).getOrCreateCache(ccfg);

        run(cache, "ALTER TABLE City ADD COLUMN population int");

        run(cache, "CREATE INDEX cidx1 ON City(population)");

        run(cache, "CREATE INDEX cidx2 ON City(name)");

        run(cache, "INSERT INTO City(id, name, population, state) values (5, 'New York', 15000000, 'New York')," +
            "(7, 'Denver', 3000000, 'Colorado')");

        List<List<?>> res = run(cache, "SELECT p.name from Person p join City c on p.city = c.name where " +
            "c.population > 5000000 order by p.name");

        assertEquals(2, res.size());

        assertEquals(Collections.singletonList("Ann Pierce"), res.get(0));

        assertEquals(Collections.singletonList("John Doe"), res.get(1));

        run(cache, "ALTER TABLE Person ADD COLUMN age int");

        run(cache, "UPDATE Person SET age = (5 - id) * 10");

        res = run(cache, "SELECT p.name from Person p join City c on p.city = c.name where " +
            "c.population > 5000000 and age < 40");

        assertEquals(1, res.size());

        assertEquals(Collections.singletonList("Ann Pierce"), res.get(0));

        run(cache, "CREATE INDEX pidx2 on Person(age desc)");

        run(cache, "DROP INDEX pidx2");
        run(cache, "DROP INDEX pidx1");
        run(cache, "DROP INDEX cidx2");
        run(cache, "DROP INDEX cidx1");

        run(cache, "DELETE FROM Person where age > 10");

        assertEquals(0, cache.size());

        ignite(nodeIndex()).destroyCache("City");
    }

    /**
     * Test that we can add columns dynamically to tables associated with non dynamic caches as well.
     */
    public void testAddColumnToNonDynamicCache() {
        run("ALTER TABLE \"idx\".PERSON ADD COLUMN CITY varchar");

        doSleep(500);

        QueryField c = c("CITY", String.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "idx", "PERSON", c);
    }

    /**
     * Test that we can add columns dynamically to tables associated with non dynamic caches storing user types as well.
     */
    @SuppressWarnings("unchecked")
    public void testAddColumnToNonDynamicCacheWithRealValueType() {
        CacheConfiguration<Integer, City> ccfg = defaultCacheConfiguration().setName("City")
            .setIndexedTypes(Integer.class, City.class);

        IgniteCache<Integer, ?> cache = ignite(nodeIndex()).getOrCreateCache(ccfg);

        run(cache, "ALTER TABLE \"City\".City ADD COLUMN population int");

        doSleep(500);

        QueryField c = c("POPULATION", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "City", "CITY", c);

        run(cache, "INSERT INTO \"City\".City (_key, id, name, state, population) values " +
            "(1, 1, 'Washington', 'DC', 2500000)");

        List<List<?>> res = run(cache, "select _key, id, name, state, population from \"City\".City");

        assertEquals(Collections.singletonList(Arrays.asList(1, 1, "Washington", "DC", 2500000)), res);

        if (!Boolean.valueOf(GridTestProperties.getProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER))) {
            City city = (City)cache.get(1);

            assertEquals(1, city.id());
            assertEquals("Washington", city.name());
            assertEquals("DC", city.state());
        }
        else {
            BinaryObject city = (BinaryObject)cache.withKeepBinary().get(1);

            assertEquals(1, (int)city.field("id"));
            assertEquals("Washington", (String)city.field("name"));
            assertEquals("DC", (String)city.field("state"));
            assertEquals(2500000, (int)city.field("population"));
        }

        cache.destroy();
    }

    /**
     * Test addition of column with not null constraint.
     */
    public void testAddNotNullColumn() {
        run("ALTER TABLE Person ADD COLUMN age int NOT NULL");

        doSleep(500);

        QueryField c = new QueryField("AGE", Integer.class.getName(), false);

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, QueryUtils.DFLT_SCHEMA, "PERSON", c);
    }

    /**
     * Test addition of column explicitly defined as nullable.
     */
    public void testAddNullColumn() {
        run("ALTER TABLE Person ADD COLUMN age int NULL");

        doSleep(500);

        QueryField c = new QueryField("AGE", Integer.class.getName(), true);

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, QueryUtils.DFLT_SCHEMA, "PERSON", c);
    }

    /**
     * @return Node index to run queries on.
     */
    protected abstract int nodeIndex();

    /**
     * Run specified statement expected to throw {@code IgniteSqlException} with expected specified message.
     * @param sql Statement.
     * @param msg Expected message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void assertThrows(final String sql, String msg) {
        assertThrows(grid(nodeIndex()), sql, msg);
    }

    /**
     * Execute SQL command and return resulting dataset.
     * @param sql Statement.
     * @return result.
     */
    protected List<List<?>> run(String sql) {
        return run(grid(nodeIndex()), sql);
    }

    /** City class. */
    private final static class City {
        /** City id. */
        @QuerySqlField
        private int id;

        /** City name. */
        @QuerySqlField
        private String name;

        /** City state. */
        @QuerySqlField
        private String state;

        /**
         * @return City id.
         */
        public int id() {
            return id;
        }

        /**
         * @param id City id.
         */
        public void id(int id) {
            this.id = id;
        }

        /**
         * @return City name.
         */
        public String name() {
            return name;
        }

        /**
         * @param name City name.
         */
        public void name(String name) {
            this.name = name;
        }

        /**
         * @return City state.
         */
        public String state() {
            return state;
        }

        /**
         * @param state City state.
         */
        public void state(String state) {
            this.state = state;
        }
    }
}
