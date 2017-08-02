package org.apache.ignite.internal.processors.cache.index;

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

        run(CREATE_SQL);
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
        run(DROP_SQL);

        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test column addition to the end of the columns list.
     */
    public void testAddColumnSimple() {
        run("ALTER TABLE Person ADD COLUMN age int");

        doSleep(500);

        QueryField c = c("AGE", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", "NAME", c);
    }

    /**
     * Test column addition before specified column.
     */
    public void testAddColumnBefore() {
        run("ALTER TABLE Person ADD COLUMN age int before id");

        doSleep(500);

        QueryField c = c("AGE", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", null, c);
    }

    /**
     * Test column addition after specified column.
     */
    public void testAddColumnAfter() {
        run("ALTER TABLE Person ADD COLUMN age int after id");

        doSleep(500);

        QueryField c = c("AGE", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", "ID", c);
    }

    /**
     * Test column addition to the end of the columns list.
     */
    public void testAddFewColumnsSimple() {
        run("ALTER TABLE Person ADD COLUMN (age int, \"city\" varchar)");

        doSleep(500);

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", "NAME",
                c("AGE", Integer.class.getName()),
                c("city", String.class.getName()));
    }

    /**
     * Test column addition before specified column.
     */
    public void testAddFewColumnsBefore() {
        run("ALTER TABLE Person ADD COLUMN (age int, \"city\" varchar) before id");

        doSleep(500);

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", null,
                c("AGE", Integer.class.getName()),
                c("city", String.class.getName()));
    }

    /**
     * Test column addition after specified column.
     */
    public void testAddFewColumnsAfter() {
        run("ALTER TABLE Person ADD COLUMN (age int, \"city\" varchar) after id");

        doSleep(500);

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", "ID",
                c("AGE", Integer.class.getName()),
                c("city", String.class.getName()));
    }

    /**
     * Test {@code IF EXISTS} handling.
     */
    public void testIfTableExists() {
        run("ALTER TABLE if exists City ADD COLUMN population int after name");
    }

    /**
     * Test {@code IF NOT EXISTS} handling.
     */
    public void testIfColumnNotExists() {
        run("ALTER TABLE Person ADD COLUMN if not exists name varchar after id");
    }

    /**
     * Test {@code IF NOT EXISTS} handling.
     */
    public void testDuplicateColumnName() {
        assertThrows("ALTER TABLE Person ADD COLUMN name varchar after id",
            "Column already exists [tblName=PERSON, colName=NAME]");
    }

    /**
     * Test behavior in case of missing table.
     */
    public void testMissingTable() {
        assertThrows("ALTER TABLE City ADD COLUMN name varchar after id", "Table doesn't exist: CITY");
    }

    /**
     * Test missing "before" column..
     */
    public void testMissingBeforeColumn() {
        assertThrows("ALTER TABLE Person ADD COLUMN city varchar before x",
            "Column \"X\" not found");
    }

    /**
     * Test missing "after" column..
     */
    public void testMissingAfterColumn() {
        assertThrows("ALTER TABLE Person ADD COLUMN city varchar after x",
            "Column \"X\" not found");
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

        ignite(nodeIndex()).cache("City").destroy();
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
