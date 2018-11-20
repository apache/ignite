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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.h2.jdbc.JdbcSQLException;

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
     * Check table state on default node.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns to look for.
     * @return Number of other columns.
     * @throws SQLException if failed.
     */
    private int checkTableState(String schemaName, String tblName, QueryField... cols) throws SQLException {
        return checkTableState(grid(nodeIndex()), schemaName, tblName, cols);
    }

    /**
     * Test column addition to the end of the columns list.
     */
    public void testAddColumnSimple() throws SQLException {
        run("ALTER TABLE Person ADD COLUMN age int");

        doSleep(500);

        QueryField c = c("AGE", Integer.class.getName());

        checkTableState(QueryUtils.DFLT_SCHEMA, "PERSON", c);
    }

    /**
     * Test column addition to the end of the columns list.
     */
    public void testAddFewColumnsSimple() throws SQLException {
        run("ALTER TABLE Person ADD COLUMN (age int, \"city\" varchar)");

        doSleep(500);

        checkTableState(QueryUtils.DFLT_SCHEMA, "PERSON", c("AGE", Integer.class.getName()),
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

        run(cache, "INSERT INTO City(id, name, population, state_name) values (5, 'New York', 15000000, 'New York')," +
            "(7, 'Denver', 3000000, 'Colorado')");

        run(cache, "ALTER TABLE City DROP COLUMN state_name");

        List<List<?>> res1 = run(cache, "SELECT * from City c " +
            "WHERE c.population > 5000000");

        assertEquals(res1.get(0).size(), 3);

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
    public void testAddColumnToNonDynamicCache() throws SQLException {
        run("ALTER TABLE \"idx\".PERSON ADD COLUMN CITY varchar");

        doSleep(500);

        QueryField c = c("CITY", String.class.getName());

        checkTableState("idx", "PERSON", c);
    }

    /**
     * Test that we can add columns dynamically to tables associated with non dynamic caches storing user types as well.
     */
    @SuppressWarnings("unchecked")
    public void testAddColumnToNonDynamicCacheWithRealValueType() throws SQLException {
        CacheConfiguration<Integer, City> ccfg = defaultCacheConfiguration().setName("City")
            .setIndexedTypes(Integer.class, City.class);

        IgniteCache<Integer, ?> cache = ignite(nodeIndex()).getOrCreateCache(ccfg);

        run(cache, "ALTER TABLE \"City\".City ADD COLUMN population int");

        doSleep(500);

        QueryField c = c("POPULATION", Integer.class.getName());

        checkTableState("City", "CITY", c);

        run(cache, "INSERT INTO \"City\".City (_key, id, name, state_name, population) values " +
            "(1, 1, 'Washington', 'DC', 2500000)");

        List<List<?>> res = run(cache, "select _key, id, name, state_name, population from \"City\".City");

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
     * Tests that we can add dynamically UUID column to tables.
     *
     * @throws SQLException If failed.
     */
    @SuppressWarnings("unchecked")
    public void testAddColumnUUID() throws SQLException {
        CacheConfiguration<Integer, Object> ccfg = defaultCacheConfiguration().setName("GuidTest")
                .setIndexedTypes(Integer.class, GuidTest.class);

        Random rnd = new Random();

        IgniteCache<Integer, Object> cache = ignite(nodeIndex()).getOrCreateCache(ccfg);

        run(cache, "ALTER TABLE \"GuidTest\".GuidTest ADD COLUMN GUID UUID");
        run(cache, "ALTER TABLE \"GuidTest\".GuidTest ADD COLUMN DATA BINARY(128)");

        doSleep(500);

        QueryField c1 = c("GUID", Object.class.getName());
        QueryField c2 = c("DATA", byte[].class.getName());

        checkTableState("GuidTest", "GUIDTEST", c1, c2);

        UUID guid1 = UUID.randomUUID();
        UUID guid2 = UUID.randomUUID();

        // Populate random data for BINARY field.
        byte[] data1 = new byte[128];
        rnd.nextBytes(data1);
        byte[] data2 = new byte[128];
        rnd.nextBytes(data2);

        run(cache, "INSERT INTO \"GuidTest\".GuidTest (_key, id, guid, data) values " +
                "(1, 1, ?, ?)", guid1.toString(), data1);

        cache.put(2, new GuidTest(2, guid2, data2));

        List<List<?>> res = run(cache, "select _key, id, guid from \"GuidTest\".GuidTest order by id");

        assertEquals(Arrays.asList(Arrays.asList(1, 1, guid1), Arrays.asList(2, 2, guid2)), res);

        // Additional check for BINARY field content.
        res = run(cache, "select data from \"GuidTest\".GuidTest order by id");

        assertTrue(Arrays.equals(data1, (byte[])res.get(0).get(0)));
        assertTrue(Arrays.equals(data2, (byte[])res.get(1).get(0)));

        if (!Boolean.valueOf(GridTestProperties.getProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER))) {
            GuidTest val1 = (GuidTest)cache.get(1);
            GuidTest val2 = (GuidTest)cache.get(2);

            assertEquals(guid1, val1.guid());
            assertEquals(guid2, val2.guid());
            assertTrue(Arrays.equals(data1, val1.data()));
            assertTrue(Arrays.equals(data2, val2.data()));
        }
        else {
            BinaryObject val1 = (BinaryObject)cache.withKeepBinary().get(1);
            BinaryObject val2 = (BinaryObject)cache.withKeepBinary().get(2);

            assertEquals(guid1, val1.field("guid"));
            assertEquals(guid2, val2.field("guid"));
            assertTrue(Arrays.equals(data1, val1.field("data")));
            assertTrue(Arrays.equals(data2, val2.field("data")));
        }

        cache.destroy();
    }

    /**
     * Test addition of column with not null constraint.
     */
    public void testAddNotNullColumn() throws SQLException {
        run("ALTER TABLE Person ADD COLUMN age int NOT NULL");

        doSleep(500);

        QueryField c = new QueryField("AGE", Integer.class.getName(), false);

        checkTableState(QueryUtils.DFLT_SCHEMA, "PERSON", c);
    }

    /**
     * Test addition of column explicitly defined as nullable.
     */
    public void testAddNullColumn() throws SQLException {
        run("ALTER TABLE Person ADD COLUMN age int NULL");

        doSleep(500);

        QueryField c = new QueryField("AGE", Integer.class.getName(), true);

        checkTableState(QueryUtils.DFLT_SCHEMA, "PERSON", c);
    }

    /**
     * Test that {@code ADD COLUMN} fails for non dynamic table that has flat value.
     */
    @SuppressWarnings({"unchecked", "ThrowFromFinallyBlock"})
    public void testTestAlterTableOnFlatValueNonDynamicTable() {
        CacheConfiguration c =
            new CacheConfiguration("ints").setIndexedTypes(Integer.class, Integer.class)
                .setSqlSchema(QueryUtils.DFLT_SCHEMA);

        try {
            grid(nodeIndex()).getOrCreateCache(c);

            doTestAlterTableOnFlatValue("INTEGER");
        }
        finally {
            grid(nodeIndex()).destroyCache("ints");
        }
    }

    /**
     * Test that {@code ADD COLUMN} fails for dynamic table that has flat value.
     */
    @SuppressWarnings({"unchecked", "ThrowFromFinallyBlock"})
    public void testTestAlterTableOnFlatValueDynamicTable() {
        try {
            run("CREATE TABLE TEST (id int primary key, x varchar) with \"wrap_value=false\"");

            doTestAlterTableOnFlatValue("TEST");
        }
        finally {
            run("DROP TABLE TEST");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumn() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, a INT, b CHAR)");

            assertEquals(0, checkTableState(QueryUtils.DFLT_SCHEMA, "TEST",
                new QueryField("ID", Integer.class.getName(), true),
                new QueryField("A", Integer.class.getName(), true),
                new QueryField("B", String.class.getName(), true)));

            run("ALTER TABLE test DROP COLUMN a");

            assertEquals(0, checkTableState(QueryUtils.DFLT_SCHEMA, "TEST",
                new QueryField("ID", Integer.class.getName(), true),
                new QueryField("B", String.class.getName(), true)));

            run("ALTER TABLE test DROP COLUMN IF EXISTS a");

            assertThrowsAnyCause("ALTER TABLE test DROP COLUMN a", JdbcSQLException.class, "Column \"A\" not found");
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDroppedColumnMeta() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, a INT, b CHAR)");

            QueryField fld = getColumnMeta(grid(nodeIndex()), QueryUtils.DFLT_SCHEMA, "TEST", "A");

            assertEquals("A", fld.name());
            assertEquals(Integer.class.getName(), fld.typeName());

            run("ALTER TABLE test DROP COLUMN a");

            assertNull(getColumnMeta(grid(nodeIndex()), QueryUtils.DFLT_SCHEMA, "TEST", "A"));
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropMultipleColumns() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, a INT, b CHAR, c INT)");

            assertEquals(0, checkTableState(QueryUtils.DFLT_SCHEMA, "TEST",
                new QueryField("ID", Integer.class.getName(), true),
                new QueryField("A", Integer.class.getName(), true),
                new QueryField("B", String.class.getName(), true),
                new QueryField("C", Integer.class.getName(), true)));

            run("ALTER TABLE test DROP COLUMN a, c");

            assertEquals(0, checkTableState(QueryUtils.DFLT_SCHEMA, "TEST",
                new QueryField("ID", Integer.class.getName(), true),
                new QueryField("B", String.class.getName(), true)));
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropNonExistingColumn() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, a INT)");

            assertThrowsAnyCause("ALTER TABLE test DROP COLUMN b", JdbcSQLException.class, "Column \"B\" not found");
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnNonExistingTable() throws Exception {
        assertThrowsAnyCause("ALTER TABLE nosuchtable DROP COLUMN a", JdbcSQLException.class,
            "Table \"NOSUCHTABLE\" not found");
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnIfTableExists() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, a INT, b CHAR)");

            run("ALTER TABLE IF EXISTS test DROP COLUMN a");

            assertEquals(0, checkTableState(QueryUtils.DFLT_SCHEMA, "TEST",
                new QueryField("ID", Integer.class.getName(), true),
                new QueryField("B", String.class.getName(), true)));
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnIfExists() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, a INT)");

            run("ALTER TABLE IF EXISTS test DROP COLUMN IF EXISTS a");

            run("ALTER TABLE IF EXISTS test DROP COLUMN IF EXISTS b");

            assertEquals(0, checkTableState(QueryUtils.DFLT_SCHEMA, "TEST",
                new QueryField("ID", Integer.class.getName(), true)));
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnIndexPresent() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, a INT, b INT)");

            run("CREATE INDEX b_index ON test(b)");

            assertThrows("ALTER TABLE test DROP COLUMN b",
                "Cannot drop column \"B\" because an index exists (\"B_INDEX\") that uses the column.");

            run("DROP INDEX b_index");

            run("ALTER TABLE test DROP COLUMN b");

            assertEquals(0, checkTableState(QueryUtils.DFLT_SCHEMA, "TEST",
                new QueryField("ID", Integer.class.getName(), true),
                new QueryField("A", Integer.class.getName(), true)));
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnOnRealClassValuedTable() throws Exception {
        try {
            run("CREATE TABLE test (id INT PRIMARY KEY, x VARCHAR) with \"wrap_value=false\"");

            assertThrows("ALTER TABLE test DROP COLUMN x",
                "Cannot drop column(s) because table was created with WRAP_VALUE=false option.");
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnThatIsPartOfKey() throws Exception {
        try {
            run("CREATE TABLE test(id INT, a INT, b CHAR, PRIMARY KEY(id, a))");

            assertThrows("ALTER TABLE test DROP COLUMN a",
                "Cannot drop column \"A\" because it is a part of a cache key");
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnThatIsKey() throws Exception {
        try {
            run("CREATE TABLE test(id INT PRIMARY KEY, a INT, b CHAR)");

            assertThrows("ALTER TABLE test DROP COLUMN id",
                "Cannot drop column \"ID\" because it represents an entire cache key");
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnThatIsValue() throws Exception {
        try {
            run("CREATE TABLE test(id INT PRIMARY KEY, a INT, b CHAR)");

            assertThrows("ALTER TABLE test DROP COLUMN _val",
                "Cannot drop column \"_VAL\" because it represents an entire cache value");
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     * Test that we can drop columns dynamically from tables associated
     * with non dynamic caches storing user types as well.
     *
     * @throws SQLException if failed.
     */
    @SuppressWarnings("unchecked")
    public void testDropColumnFromNonDynamicCacheWithRealValueType() throws SQLException {
        CacheConfiguration<Integer, City> ccfg = defaultCacheConfiguration().setName("City")
            .setIndexedTypes(Integer.class, City.class);

        IgniteCache<Integer, ?> cache = ignite(nodeIndex()).getOrCreateCache(ccfg);

        run(cache, "INSERT INTO \"City\".City (_key, id, name, state_name) VALUES " +
            "(1, 1, 'Washington', 'DC')");

        run(cache, "ALTER TABLE \"City\".City DROP COLUMN state_name");

        doSleep(500);

        QueryField c = c("NAME", String.class.getName());

        checkTableState("City", "CITY", c);

        run(cache, "INSERT INTO \"City\".City (_key, id, name) VALUES " +
            "(2, 2, 'New York')");

        assertThrowsAnyCause("SELECT state_name FROM \"City\".City",
            JdbcSQLException.class, "Column \"STATE_NAME\" not found");

        List<List<?>> res = run(cache, "SELECT _key, id, name FROM \"City\".City WHERE id = 1");

        assertEquals(Collections.singletonList(Arrays.asList(1, 1, "Washington")), res);

        res = run(cache, "SELECT * FROM \"City\".City WHERE id = 2");

        assertEquals(Collections.singletonList(Arrays.asList(2, "New York")), res);

        if (!Boolean.valueOf(GridTestProperties.getProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER))) {
            City city = (City)cache.get(1);

            assertEquals(1, city.id());
            assertEquals("Washington", city.name());
            assertEquals("DC", city.state());

            city = (City)cache.get(2);

            assertEquals(2, city.id());
            assertEquals("New York", city.name());
            assertEquals(null, city.state());
        }
        else {
            BinaryObject city = (BinaryObject)cache.withKeepBinary().get(1);

            assertEquals(1, (int)city.field("id"));
            assertEquals("Washington", (String)city.field("name"));
            assertEquals("DC", (String)city.field("state"));

            city = (BinaryObject)cache.withKeepBinary().get(2);

            assertEquals(2, (int)city.field("id"));
            assertEquals("New York", (String)city.field("name"));
            assertEquals(null, (String)city.field("state"));
        }

        cache.destroy();
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testDropColumnPriorToIndexedColumn() throws Exception {
        try {
            run("CREATE TABLE test(id INT PRIMARY KEY, a CHAR, b INT)");

            run("CREATE INDEX idxB ON test(b)");

            run("INSERT INTO test VALUES(1, 'one', 11), (2, 'two', 22), (3, 'three', 33)");

            List<List<?>> res = run("SELECT * FROM test WHERE b > 0 ORDER BY b");

            assertEquals(3, res.size());
            assertEquals(3, res.get(0).size());

            run("ALTER TABLE test DROP COLUMN a");

            res = run("SELECT * FROM test WHERE b > 0 ORDER BY b");

            assertEquals(3, res.size());
            assertEquals(2, res.get(0).size());

            assertEquals(1, res.get(0).get(0));
            assertEquals(11, res.get(0).get(1));
        }
        finally {
            run("DROP TABLE IF EXISTS test");
        }
    }

    /**
     * Test that {@code ADD COLUMN} fails for tables that have flat value.
     * @param tblName table name.
     */
    private void doTestAlterTableOnFlatValue(String tblName) {
        assertThrows("ALTER TABLE " + tblName + " ADD COLUMN y varchar",
            "Cannot add column(s) because table was created with WRAP_VALUE=false option.");
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
     * Run specified statement expected to throw an exception of specified class and message.
     *
     * @param sql Statement.
     * @param cls Expected exception class.
     * @param msg Expected message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void assertThrowsAnyCause(final String sql, Class<? extends Throwable> cls, String msg) {
        assertThrowsAnyCause(grid(nodeIndex()), sql, cls, msg);
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
        @QuerySqlField(name = "state_name")
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

    /**  */
    private final static class GuidTest {
        /** */
        @QuerySqlField
        private int id;

        /** */
        private UUID guid;

        /** */
        private byte[] data;

        /**
         * @param id   Id.
         * @param guid Guid.
         * @param data Data.
         */
        public GuidTest(int id, UUID guid, byte[] data) {
            this.id = id;
            this.guid = guid;
            this.data = data;
        }

        /**
         * @return Id.
         */
        public int id() {
            return id;
        }

        /**
         * @return Guid.
         */
        public UUID guid() {
            return guid;
        }

        /**
         * @return Data.
         */
        public byte[] data() {
            return data;
        }
    }
}
