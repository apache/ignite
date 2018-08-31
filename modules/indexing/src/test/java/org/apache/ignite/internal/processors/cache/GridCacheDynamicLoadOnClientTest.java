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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Test lazy cache start on client nodes.
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

    /** Number of nodes. */
    private static final int NODES = 2;

    /** Client or server mode for configuration. */
    private boolean client;

    /** Instance of client node. */
    private static IgniteEx clientNode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientMode(client);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = false;

        startGridsMultiThreaded(NODES - 1);

        client = true;

        clientNode = startGrid(NODES - 1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clientNode.destroyCache(PERSON_CACHE);

        createAndFillServerCache();
    }

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Test from client node batch merge through JDBC.
     *
     * @throws Exception If failure.
     */
    public void testBatchMerge() throws Exception {
        Connection con = connect(clientNode, null);

        final int BATCH_SIZE = 7;

        try (Statement stmt = con.createStatement()) {
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
    public void testClientJdbcDelete() throws Exception {
        Connection con = connect(clientNode, null);

        try (Statement stmt = con.createStatement()) {
            int cnt = stmt.executeUpdate(("DELETE " + FULL_TABLE_NAME + " WHERE _key=1"));
            Assert.assertEquals(1, cnt);
        }

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME);

        Assert.assertEquals(CACHE_ELEMENT_COUNT - 1, clientNode.cache(PERSON_CACHE).query(qry).getAll().size());
    }

    /**
     * Test from client node to insert into cache through JDBC.
     *
     * @throws Exception If failure.
     */
    public void testClientJdbcInsert() throws Exception {
        Connection con = connect(clientNode, null);

        try (Statement stmt = con.createStatement()) {
            int cnt = stmt.executeUpdate(("INSERT INTO " + FULL_TABLE_NAME + "(_key, name, orgId) VALUES(1000,'new_name', 10000)"));
            Assert.assertEquals(1, cnt);
        }

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME);

        Assert.assertEquals(CACHE_ELEMENT_COUNT + 1, clientNode.cache(PERSON_CACHE).query(qry).getAll().size());
    }

    /**
     * Test from client node to update cache elements through JDBC.
     *
     * @throws Exception If failure.
     */
    public void testClientJdbcUpdate() throws Exception {
        Connection con = connect(clientNode, null);

        try (Statement stmt = con.createStatement()) {
            int cnt = stmt.executeUpdate(("UPDATE " + FULL_TABLE_NAME + " SET name = 'new_name'"));
            Assert.assertEquals(CACHE_ELEMENT_COUNT, cnt);
        }

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME + " WHERE name = 'new_name'");

        Assert.assertEquals(CACHE_ELEMENT_COUNT, clientNode.cache(PERSON_CACHE).query(qry).getAll().size());
    }

    /**
     * Test from client node to get cache elements through JDBC.
     *
     * @throws Exception If failure.
     */
    public void testClientJdbc() throws Exception {
        Connection con = connect(clientNode, null);
        try (Statement st = con.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT count(*) FROM " + FULL_TABLE_NAME);

            rs.next();

            Assert.assertEquals(CACHE_ELEMENT_COUNT, rs.getInt(1));
        }
    }

    /**
     * Test from client node to put cache elements through cache API.
     */
    public void testClientPut() {
        clientNode.cache(PERSON_CACHE).put(-100, new Person(-100, "name-"));

        Assert.assertEquals(CACHE_ELEMENT_COUNT + 1, clientNode.cache(PERSON_CACHE).size());
    }

    /**
     * Test from client node to get cache elements through cache API.
     */
    public void testClientSqlFieldsQuery() {
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME);

        Assert.assertEquals(CACHE_ELEMENT_COUNT, clientNode.cache(PERSON_CACHE).query(qry).getAll().size());
    }

    /**
     * Test from client node to get cache elements through cache query API with flag local
     */
    public void testClientSqlFieldsLocalQuery() {
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM " + FULL_TABLE_NAME).setLocal(true);

        Assert.assertEquals(0, clientNode.cache(PERSON_CACHE).query(qry).getAll().size());
    }

    /**
     * Test from client node to get cache elements through cache API.
     */
    public void testClientSqlQuery() {
        SqlQuery<Integer, Person> qry = new SqlQuery<>(PERSON_CACHE, "FROM " + PERSON_CACHE);

        Assert.assertEquals(CACHE_ELEMENT_COUNT,
            clientNode.cache(PERSON_CACHE).query(qry).getAll().size());
    }

    /**
     * Create cache at server node and put some values into the cache.
     */
    private void createAndFillServerCache() {
        IgniteEx srvNode = grid(0);

        srvNode.createCache(cacheConfiguration());

        for (int i = 0; i < CACHE_ELEMENT_COUNT; i++)
            srvNode.cache(PERSON_CACHE).put(i, new Person(i, "name-" + i));
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<?,?> cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration(PERSON_CACHE);

        ccfg.setSqlSchema(PERSON_SCHEMA);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        QueryEntity person = new QueryEntity();
        person.setKeyType(Integer.class.getName());
        person.setValueType(Person.class.getName());
        person.addQueryField("orgId", Integer.class.getName(), null);
        person.addQueryField("id", Integer.class.getName(), null);
        person.addQueryField("name", String.class.getName(), null);
        person.setIndexes(F.asList(new QueryIndex("orgId"), new QueryIndex("id"), new QueryIndex("name")));

        ccfg.setQueryEntities(F.asList(person));

        return ccfg;
    }

    /**
     * Create SQL connection to node specified as argument
     *
     * @param node Node to connect.
     * @param params Additional connection parameters
     * @return Connection.
     * @throws SQLException In case of failure.
     */
    private static Connection connect(IgniteEx node, String params) throws SQLException {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class) {
                cliLsnrRec = rec;

                break;
            }
        }

        String connStr = "jdbc:ignite:thin://127.0.0.1:" + cliLsnrRec.port();

        if (!F.isEmpty(params))
            connStr += "/?" + params;

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
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }

}
