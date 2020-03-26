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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test lazy cache start on client nodes with inmemory cache.
 */
public class GridCacheDynamicLoadOnClientTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String PERSON_CACHE = "Person";

    /** SQL schema name. */
    private static final String PERSON_SCHEMA = "test";

    /** Number of element to add into cache. */
    private static final int CACHE_ELEMENT_COUNT = 10;

    /** Full table name. */
    private static final String FULL_TABLE_NAME = PERSON_SCHEMA + "." + PERSON_CACHE;

    /** Instance of client node. */
    private static IgniteEx clientNode;

    /** Instance of client node. */
    private static IgniteEx srvNode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srvNode = (IgniteEx)startGridsMultiThreaded(1);

        clientNode = startClientGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clientNode.destroyCache(PERSON_CACHE);

        createAndFillServerCache();
    }

    /**
     * Test from client node batch merge through JDBC.
     *
     * @throws Exception If failure.
     */
    @Test
    public void testBatchMerge() throws Exception {
        final int BATCH_SIZE = 7;

        try (Connection con = connect(clientNode);
             Statement stmt = con.createStatement()) {
            for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
                stmt.addBatch("merge into " + FULL_TABLE_NAME + " (_key, name, orgId) values (" +
                    (100 + idx) + "," +
                    "'" + "batch-" + idx + "'" + "," +
                    idx +
                    ")");
            }

            int[] updCnts = stmt.executeBatch();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);
        }
    }

    /**
     * Test from client node to delete cache elements through JDBC.
     *
     * @throws Exception If failure.
     */
    @Test
    public void testClientJdbcDelete() throws Exception {
        try (Connection con = connect(clientNode);
             Statement stmt = con.createStatement()) {
            int cnt = stmt.executeUpdate(("DELETE " + FULL_TABLE_NAME + " WHERE _key=1"));

            Assert.assertEquals(1, cnt);
        }

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME);

        Assert.assertEquals(CACHE_ELEMENT_COUNT - 1, getDefaultCacheOnClient().query(qry).getAll().size());
    }

    /**
     * Test from client node to insert into cache through JDBC.
     *
     * @throws Exception If failure.
     */
    @Test
    public void testClientJdbcInsert() throws Exception {
        try (Connection con = connect(clientNode);
             Statement stmt = con.createStatement()) {
            int cnt = stmt.executeUpdate(
                "INSERT INTO " + FULL_TABLE_NAME + "(_key, name, orgId) VALUES(1000,'new_name', 10000)");

            Assert.assertEquals(1, cnt);
        }

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME);

        Assert.assertEquals(CACHE_ELEMENT_COUNT + 1, getDefaultCacheOnClient().query(qry).getAll().size());
    }

    /**
     * Test from client node to update cache elements through JDBC.
     *
     * @throws Exception If failure.
     */
    @Test
    public void testClientJdbcUpdate() throws Exception {
        try (Connection con = connect(clientNode);
             Statement stmt = con.createStatement()) {
            int cnt = stmt.executeUpdate(("UPDATE " + FULL_TABLE_NAME + " SET name = 'new_name'"));

            Assert.assertEquals(CACHE_ELEMENT_COUNT, cnt);
        }

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME + " WHERE name = 'new_name'");

        Assert.assertEquals(CACHE_ELEMENT_COUNT, getDefaultCacheOnClient().query(qry).getAll().size());
    }

    /**
     * Test from client node to get cache elements through JDBC.
     *
     * @throws Exception If failure.
     */
    @Test
    public void testClientJdbc() throws Exception {
        try (Connection con = connect(clientNode);
             Statement st = con.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME);

            rs.next();

            Assert.assertEquals(CACHE_ELEMENT_COUNT, rs.getInt(1));
        }
    }

    /**
     * Test from client node to put cache elements through cache API.
     */
    @Test
    public void testClientPut() {
        clientNode.cache(PERSON_CACHE).put(-100, new Person(-100, "name-"));

        Assert.assertEquals(CACHE_ELEMENT_COUNT + 1, clientNode.cache(PERSON_CACHE).size());
    }

    /**
     * Test DDL operation for not started cache on client node.
     */
    @Test
    public void testCreateIdxOnClient() {
        getDefaultCacheOnClient().query(new SqlFieldsQuery("CREATE INDEX IDX_11 ON " + FULL_TABLE_NAME + " (name asc)")).getAll();
    }

    /**
     * Test DDL operation for not started cache on client node.
     */
    @Test
    public void testDropIdxOnClient() {
        srvNode.getOrCreateCache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX IDX_TST ON " + FULL_TABLE_NAME + " (name desc)")).getAll();

        //Due to client receive created index asynchronously we need add the ugly sleep.
        doSleep(2000);

        getDefaultCacheOnClient().query(new SqlFieldsQuery("DROP INDEX " + PERSON_SCHEMA + ".IDX_TST")).getAll();
    }


    /**
     * Test from client node to get cache elements through cache API.
     */
    @Test
    public void testClientSqlFieldsQuery() {
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME);

        Assert.assertEquals(CACHE_ELEMENT_COUNT, getDefaultCacheOnClient().query(qry).getAll().size());
    }

    /**
     * Test from client node to get cache elements through cache API.
     */
    @Test
    public void testClientSqlQuery() {
        SqlQuery<Integer, Person> qry = new SqlQuery<>(PERSON_CACHE, "FROM " + PERSON_CACHE);

        Assert.assertEquals(CACHE_ELEMENT_COUNT, clientNode.getOrCreateCache(PERSON_CACHE).query(qry).getAll().size());
    }

    /**
     * @return Default cache on client node.
     */
    private IgniteCache getDefaultCacheOnClient() {
        return clientNode.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /**
     * Create cache at server node and put some values into the cache.
     */
    private void createAndFillServerCache() {
        srvNode.createCache(cacheConfiguration());

        for (int i = 0; i < CACHE_ELEMENT_COUNT; i++)
            srvNode.cache(PERSON_CACHE).put(i, new Person(i, "name-" + i));
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration(PERSON_CACHE);

        ccfg.setSqlSchema(PERSON_SCHEMA);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        QueryEntity person = new QueryEntity();
        person.setKeyType(Integer.class.getName());
        person.setValueType(Person.class.getName());
        person.addQueryField("orgId", Integer.class.getName(), null);
        person.addQueryField("name", String.class.getName(), null);
        person.setIndexes(F.asList(new QueryIndex("orgId"), new QueryIndex("name")));

        ccfg.setQueryEntities(F.asList(person));

        return ccfg;
    }

    /**
     * Create SQL connection to node specified as argument
     *
     * @param node Node to connect.
     * @return Connection.
     * @throws SQLException In case of failure.
     */
    private static Connection connect(IgniteEx node) throws SQLException {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class) {
                cliLsnrRec = rec;

                break;
            }
        }

        String connStr = "jdbc:ignite:thin://127.0.0.1:" + cliLsnrRec.port();

        return DriverManager.getConnection(connStr);
    }

    /**
     * Test object to keep in cache.
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField
        int orgId;

        /** */
        @QuerySqlField
        String name;

        /**
         * @param orgId Organization ID.
         * @param name Name.
         */
        Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
