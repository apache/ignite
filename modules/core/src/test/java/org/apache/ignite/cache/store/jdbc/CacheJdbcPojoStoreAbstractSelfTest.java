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

package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Random;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.cache.store.jdbc.model.Person;
import org.apache.ignite.cache.store.jdbc.model.Gender;
import org.apache.ignite.cache.store.jdbc.model.PersonKey;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Class for {@link CacheJdbcPojoStore} tests.
 */
public abstract class CacheJdbcPojoStoreAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** DB connection URL. */
    private static final String DFLT_CONN_URL = "jdbc:h2:mem:TestDatabase;DB_CLOSE_DELAY=-1";

    /** Organization count. */
    private static final int ORGANIZATION_CNT = 1000;

    /** Person count. */
    private static final int PERSON_CNT = 100000;

    /** Test cache name. */
    private static final String CACHE_NAME = "test-cache";

    /** Flag indicating that tests should use transactional cache. */
    private static boolean transactional;

    /** Flag indicating that tests should use primitive classes like java.lang.Integer for keys. */
    protected static boolean builtinKeys;

    /** Flag indicating that classes for keys available on class path or not. */
    private static boolean noKeyClasses;

    /** Flag indicating that classes for values available on class path or not. */
    private static boolean noValClasses;

    /** Batch size to load in parallel. */
    private static int parallelLoadThreshold;

    /**
     * @return Flag indicating that all internal SQL queries should use escaped identifiers.
     */
    protected boolean sqlEscapeAll(){
        return false;
    }

    /**
     * @return Connection to test in-memory H2 database.
     * @throws SQLException if failed to connect.
     */
    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DFLT_CONN_URL, "sa", "");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Connection conn = getConnection();

        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS Organization");
        stmt.executeUpdate("DROP TABLE IF EXISTS Person");

        stmt.executeUpdate("CREATE TABLE Organization (" +
            " id INTEGER PRIMARY KEY," +
            " name VARCHAR(50)," +
            " city VARCHAR(50))");

        stmt.executeUpdate("CREATE TABLE Person (" +
            " id INTEGER PRIMARY KEY," +
            " org_id INTEGER," +
            " birthday DATE," +
            " name VARCHAR(50)," +
            " gender VARCHAR(50))");

        conn.commit();

        U.closeQuiet(stmt);

        fillSampleDatabase(conn);

        U.closeQuiet(conn);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setMarshaller(marshaller());

        ConnectorConfiguration connCfg = new ConnectorConfiguration();
        cfg.setConnectorConfiguration(connCfg);

        return cfg;
    }

    /**
     * @return Marshaller to be used in test.
     */
    protected abstract Marshaller marshaller();

    /**
     * @return Types to be used in test.
     */
    protected JdbcType[] storeTypes() {
        JdbcType[] storeTypes = new JdbcType[2];

        storeTypes[0] = new JdbcType();
        storeTypes[0].setCacheName(CACHE_NAME);
        storeTypes[0].setDatabaseSchema("PUBLIC");
        storeTypes[0].setDatabaseTable("ORGANIZATION");

        if (builtinKeys) {
            storeTypes[0].setKeyType("java.lang.Long");
            storeTypes[0].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"));
        }
        else {
            storeTypes[0].setKeyType("org.apache.ignite.cache.store.jdbc.model.OrganizationKey" + (noKeyClasses ? "1" : ""));
            storeTypes[0].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"));
        }

        storeTypes[0].setValueType("org.apache.ignite.cache.store.jdbc.model.Organization" + (noValClasses ? "1" : ""));

        boolean escape = sqlEscapeAll();

        storeTypes[0].setValueFields(
            new JdbcTypeField(Types.INTEGER, escape ? "ID" : "Id", Integer.class, "id"),
            new JdbcTypeField(Types.VARCHAR, escape ? "NAME" : "Name", String.class, "name"),
            new JdbcTypeField(Types.VARCHAR, escape ? "CITY" : "City", String.class, "city"));

        storeTypes[1] = new JdbcType();
        storeTypes[1].setCacheName(CACHE_NAME);
        storeTypes[1].setDatabaseSchema("PUBLIC");
        storeTypes[1].setDatabaseTable("PERSON");

        if (builtinKeys) {
            storeTypes[1].setKeyType("java.lang.Integer");
            storeTypes[1].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Long.class, "id"));
        }
        else {
            storeTypes[1].setKeyType("org.apache.ignite.cache.store.jdbc.model.PersonKey" + (noKeyClasses ? "1" : ""));
            storeTypes[1].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"));
        }

        storeTypes[1].setValueType("org.apache.ignite.cache.store.jdbc.model.Person" + (noValClasses ? "1" : ""));
        storeTypes[1].setValueFields(
            new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new JdbcTypeField(Types.INTEGER, "ORG_ID", Integer.class, "orgId"),
            new JdbcTypeField(Types.DATE, "BIRTHDAY", Date.class, "birthday"),
            new JdbcTypeField(Types.VARCHAR, "NAME", String.class, "name"),
            new JdbcTypeField(Types.VARCHAR, "GENDER", Gender.class, "gender"));

        return storeTypes;
    }

    /**
     * @return Cache configuration for test.
     * @throws Exception In case when failed to create cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(transactional ? TRANSACTIONAL : ATOMIC);
        cc.setSwapEnabled(false);
        cc.setWriteBehindEnabled(false);

        CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
        storeFactory.setDialect(new H2Dialect());
        storeFactory.setTypes(storeTypes());
        storeFactory.setDataSourceFactory(new H2DataSourceFactory()); // H2 DataSource factory.
        storeFactory.setSqlEscapeAll(sqlEscapeAll());
        storeFactory.setParallelLoadCacheMinimumThreshold(parallelLoadThreshold);

        cc.setCacheStoreFactory(storeFactory);
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        return cc;
    }

    /**
     * Fill in-memory database with sample data.
     *
     * @param conn Connection to database.
     * @throws SQLException In case of filling database with sample data failed.
     */
    protected void fillSampleDatabase(Connection conn) throws SQLException {
        info("Start to fill sample database...");

        PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO Organization(id, name, city) VALUES (?, ?, ?)");

        for (int i = 0; i < ORGANIZATION_CNT; i++) {
            orgStmt.setInt(1, i);
            orgStmt.setString(2, "name" + i);
            orgStmt.setString(3, "city" + i % 10);

            orgStmt.addBatch();
        }

        orgStmt.executeBatch();

        U.closeQuiet(orgStmt);

        conn.commit();

        PreparedStatement prnStmt = conn.prepareStatement(
            "INSERT INTO Person(id, org_id, birthday, name, gender) VALUES (?, ?, ?, ?, ?)");

        Random rnd = new Random();

        for (int i = 0; i < PERSON_CNT; i++) {
            prnStmt.setInt(1, i);
            prnStmt.setInt(2, i % 100);
            prnStmt.setDate(3, Date.valueOf(String.format("%d-%d-%d", 1970 + rnd.nextInt(50), 1 + rnd.nextInt(11), 1 + rnd.nextInt(27))));
            prnStmt.setString(4, "name" + i);
            prnStmt.setString(5, Gender.random().toString());

            prnStmt.addBatch();
        }

        prnStmt.executeBatch();

        conn.commit();

        U.closeQuiet(prnStmt);

        info("Sample database prepared.");
    }

    /**
     * Start test grid with specified options.
     *
     * @param builtin {@code True} if keys are built in java types.
     * @param noKeyCls {@code True} if keys classes are not on class path.
     * @param noValCls {@code True} if values classes are not on class path.
     * @param trn {@code True} if cache should be started in transactional mode.
     * @param threshold Load batch size.
     * @throws Exception If failed to start grid.
     */
    protected void startTestGrid(boolean builtin, boolean noKeyCls, boolean noValCls, boolean trn, int threshold) throws Exception {
        builtinKeys = builtin;
        noKeyClasses = noKeyCls;
        noValClasses = noValCls;
        transactional = trn;
        parallelLoadThreshold = threshold;

        startGrid();
    }

    /**
     * Check that data was loaded correctly.
     */
    protected void checkCacheLoad() {
        IgniteCache<Object, Object> c1 = grid().cache(CACHE_NAME);

        c1.loadCache(null);

        assertEquals(ORGANIZATION_CNT + PERSON_CNT, c1.size());
    }

    /**
     * Check that data was loaded correctly.
     */
    protected void checkCacheLoadWithSql() {
        IgniteCache<Object, Object> c1 = grid().cache(CACHE_NAME);

        c1.loadCache(null, "org.apache.ignite.cache.store.jdbc.model.PersonKey", "select id, org_id, name, birthday, gender from Person");

        assertEquals(PERSON_CNT, c1.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        startTestGrid(false, false, false, false, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheAll() throws Exception {
        startTestGrid(false, false, false, false, ORGANIZATION_CNT + PERSON_CNT + 1);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheWithSql() throws Exception {
        startTestGrid(false, false, false, false, 512);

        checkCacheLoadWithSql();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheTx() throws Exception {
        startTestGrid(false, false, false, true, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheWithSqlTx() throws Exception {
        startTestGrid(false, false, false, true, 512);

        checkCacheLoadWithSql();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCachePrimitiveKeys() throws Exception {
        startTestGrid(true, false, false, false, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCachePrimitiveKeysTx() throws Exception {
        startTestGrid(true, false, false, true, 512);

        checkCacheLoad();
    }

    /**
     * Check put in cache and store it in db.
     *
     * @throws Exception If failed.
     */
    private void checkPutRemove() throws Exception {
        IgniteCache<Object, Person> c1 = grid().cache(CACHE_NAME);

        Connection conn = getConnection();
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT ID, ORG_ID, BIRTHDAY, NAME, GENDER FROM PERSON WHERE ID = ?");

            stmt.setInt(1, -1);

            ResultSet rs = stmt.executeQuery();

            assertFalse("Unexpected non empty result set", rs.next());

            U.closeQuiet(rs);

            Date testDate = Date.valueOf("2001-05-05");
            Gender testGender = Gender.random();

            Person val = new Person(-1, -2, testDate, "Person-to-test-put-insert", 999, testGender);

            Object key = builtinKeys ? Integer.valueOf(-1) : new PersonKey(-1);

            // Test put-insert.
            c1.put(key, val);

            rs = stmt.executeQuery();

            assertTrue("Unexpected empty result set", rs.next());

            assertEquals(-1, rs.getInt(1));
            assertEquals(-2, rs.getInt(2));
            assertEquals(testDate, rs.getDate(3));
            assertEquals("Person-to-test-put-insert", rs.getString(4));
            assertEquals(testGender.toString(), rs.getString(5));

            assertFalse("Unexpected more data in result set", rs.next());

            U.closeQuiet(rs);

            // Test put-update.
            testDate = Date.valueOf("2016-04-04");

            c1.put(key, new Person(-1, -3, testDate, "Person-to-test-put-update", 999, testGender));

            rs = stmt.executeQuery();

            assertTrue("Unexpected empty result set", rs.next());

            assertEquals(-1, rs.getInt(1));
            assertEquals(-3, rs.getInt(2));
            assertEquals(testDate, rs.getDate(3));
            assertEquals("Person-to-test-put-update", rs.getString(4));
            assertEquals(testGender.toString(), rs.getString(5));

            assertFalse("Unexpected more data in result set", rs.next());

            // Test remove.
            c1.remove(key);

            rs = stmt.executeQuery();

            assertFalse("Unexpected non-empty result set", rs.next());

            U.closeQuiet(rs);
        }
        finally {
            U.closeQuiet(conn);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoveBuiltIn() throws Exception {
        startTestGrid(true, false, false, false, 512);

        checkPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemove() throws Exception {
        startTestGrid(false, false, false, false, 512);

        checkPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoveTxBuiltIn() throws Exception {
        startTestGrid(true, false, false, true, 512);

        checkPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoveTx() throws Exception {
        startTestGrid(false, false, false, true, 512);

        checkPutRemove();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadNotRegisteredType() throws Exception {
        startTestGrid(false, false, false, false, 512);

        IgniteCache<Object, Object> c1 = grid().cache(CACHE_NAME);

        try {
            c1.loadCache(null, "PersonKeyWrong", "SELECT * FROM Person");
        }
        catch (CacheLoaderException e) {
            String msg = e.getMessage();

            assertTrue("Unexpected exception: " + msg,
                ("Provided key type is not found in store or cache configuration " +
                    "[cache=" + CACHE_NAME + ", key=PersonKeyWrong]").equals(msg));
        }
    }
}
