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

package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for ignite SQL system views.
 */
public class SqlSystemViewsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object ... args) {
        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }

    /**
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(String sql, Object ... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param sql Sql.
     */
    private void assertSqlError(final String sql) {
        Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() {
                execSql(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assert sqlE != null;

        assertEquals(IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlE.statusCode());
    }
    
    /**
     * Test system views modifications.
     */
    public void testModifications() throws Exception {
        startGrid();

        assertSqlError("DROP TABLE IGNITE.NODES");

        assertSqlError("TRUNCATE TABLE IGNITE.NODES");

        assertSqlError("ALTER TABLE IGNITE.NODES RENAME TO IGNITE.N");

        assertSqlError("ALTER TABLE IGNITE.NODES ADD COLUMN C VARCHAR");

        assertSqlError("ALTER TABLE IGNITE.NODES DROP COLUMN ID");

        assertSqlError("ALTER TABLE IGNITE.NODES RENAME COLUMN ID TO C");

        assertSqlError("CREATE INDEX IDX ON IGNITE.NODES(ID)");

        assertSqlError("INSERT INTO IGNITE.NODES (ID) VALUES ('-')");

        assertSqlError("UPDATE IGNITE.NODES SET ID = '-'");

        assertSqlError("DELETE IGNITE.NODES");
    }

    /**
     * Test different query modes.
     */
    public void testQueryModes() throws Exception {
        Ignite ignite = startGrid(0);
        startGrid(1);

        UUID nodeId = ignite.cluster().localNode().id();

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        String sql = "SELECT ID FROM IGNITE.NODES WHERE NODE_ORDER = 1";

        SqlFieldsQuery qry;

        qry = new SqlFieldsQuery(sql).setDistributedJoins(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setReplicatedOnly(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setLocal(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));
    }

    /**
     * Test that we can't use cache tables and system views in the same query.
     */
    public void testCacheToViewJoin() throws Exception {
        Ignite ignite = startGrid();

        ignite.createCache(new CacheConfiguration<>().setName(DEFAULT_CACHE_NAME).setQueryEntities(
            Collections.singleton(new QueryEntity(Integer.class.getName(), String.class.getName()))));

        assertSqlError("SELECT * FROM \"" + DEFAULT_CACHE_NAME + "\".String JOIN IGNITE.NODES ON 1=1");
    }

    /**
     * @param rowData Row data.
     * @param colTypes Column types.
     */
    private void assertColumnTypes(List<?> rowData, Class<?> ... colTypes) {
        for (int i = 0; i < colTypes.length; i++) {
            if (rowData.get(i) != null)
                assertEquals("Column " + i + " type", rowData.get(i).getClass(), colTypes[i]);
        }
    }

    /**
     * Test nodes system view.
     *
     * @throws Exception If failed.
     */
    public void testNodesViews() throws Exception {
        Ignite ignite1 = startGrid(getTestIgniteInstanceName(), getConfiguration());
        Ignite ignite2 = startGrid(getTestIgniteInstanceName(1), getConfiguration().setClientMode(true));
        Ignite ignite3 = startGrid(getTestIgniteInstanceName(2), getConfiguration().setDaemon(true));

        awaitPartitionMapExchange();

        List<List<?>> resAll = execSql("SELECT ID, CONSISTENT_ID, VERSION, IS_CLIENT, IS_DAEMON, " +
                "NODE_ORDER, ADDRESSES, HOSTNAMES FROM IGNITE.NODES");

        assertColumnTypes(resAll.get(0), UUID.class, String.class, String.class, Boolean.class, Boolean.class,
            Integer.class, String.class, String.class);

        assertEquals(3, resAll.size());

        List<List<?>> resSrv = execSql(
            "SELECT ID, NODE_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = FALSE AND IS_DAEMON = FALSE"
        );

        assertEquals(1, resSrv.size());

        assertEquals(ignite1.cluster().localNode().id(), resSrv.get(0).get(0));

        assertEquals(1, resSrv.get(0).get(1));

        List<List<?>> resCli = execSql(
            "SELECT ID, NODE_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = TRUE");

        assertEquals(1, resCli.size());

        assertEquals(ignite2.cluster().localNode().id(), resCli.get(0).get(0));

        assertEquals(2, resCli.get(0).get(1));

        List<List<?>> resDaemon = execSql(
            "SELECT ID, NODE_ORDER FROM IGNITE.NODES WHERE IS_DAEMON = TRUE");

        assertEquals(1, resDaemon.size());

        assertEquals(ignite3.cluster().localNode().id(), resDaemon.get(0).get(0));

        assertEquals(3, resDaemon.get(0).get(1));

        // Check index on ID column.
        assertEquals(0, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = '-'").size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            ignite1.cluster().localNode().id()).size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            ignite3.cluster().localNode().id()).size());

        // Check index on ID column with disjunction.
        assertEquals(3, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ? " +
            "OR node_order=1 OR node_order=2 OR node_order=3", ignite1.cluster().localNode().id()).size());

        // Check quick-count.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM IGNITE.NODES").get(0).get(0));

        // Check joins
        assertEquals(ignite1.cluster().localNode().id(), execSql("SELECT N1.ID FROM IGNITE.NODES N1 JOIN " +
            "IGNITE.NODES N2 ON N1.NODE_ORDER = N2.NODE_ORDER JOIN IGNITE.NODES N3 ON N2.ID = N3.ID " +
            "WHERE N3.NODE_ORDER = 1")
            .get(0).get(0));

        // Check sub-query
        assertEquals(ignite1.cluster().localNode().id(), execSql("SELECT N1.ID FROM IGNITE.NODES N1 " +
            "WHERE NOT EXISTS (SELECT 1 FROM IGNITE.NODES N2 WHERE N2.ID = N1.ID AND N2.NODE_ORDER <> 1)")
            .get(0).get(0));

        // Check node attributes view
        UUID cliNodeId = ignite2.cluster().localNode().id();

        String cliAttrName = IgniteNodeAttributes.ATTR_CLIENT_MODE;

        assertColumnTypes(execSql("SELECT NODE_ID, NAME, VALUE FROM IGNITE.NODE_ATTRIBUTES").get(0),
            UUID.class, String.class, String.class);

        assertEquals(1,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NAME = ? AND VALUE = 'true'",
                cliAttrName).size());

        assertEquals(3,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NAME = ?", cliAttrName).size());

        assertEquals(1,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = ? AND VALUE = 'true'",
                cliNodeId, cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = '-' AND NAME = ?",
                cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = '-'",
                cliNodeId).size());
    }

    /**
     * Test baseline topology system view.
     */
    public void testBaselineViews() throws Exception {
        cleanPersistenceDir();

        Ignite ignite = startGrid(getTestIgniteInstanceName(), getPdsConfiguration("node0"));
        startGrid(getTestIgniteInstanceName(1), getPdsConfiguration("node1"));

        ignite.cluster().active(true);

        List<List<?>> res = execSql("SELECT CONSISTENT_ID, ONLINE FROM IGNITE.BASELINE_NODES ORDER BY CONSISTENT_ID");

        assertColumnTypes(res.get(0), String.class, Boolean.class);

        assertEquals(2, res.size());

        assertEquals("node0", res.get(0).get(0));
        assertEquals("node1", res.get(1).get(0));

        assertEquals(true, res.get(0).get(1));
        assertEquals(true, res.get(1).get(1));

        stopGrid(getTestIgniteInstanceName(1));

        res = execSql("SELECT CONSISTENT_ID FROM IGNITE.BASELINE_NODES WHERE ONLINE = false");

        assertEquals(1, res.size());

        assertEquals("node1", res.get(0).get(0));

        Ignite ignite2 = startGrid(getTestIgniteInstanceName(2), getPdsConfiguration("node2"));

        assertEquals(2, execSql(ignite2, "SELECT CONSISTENT_ID FROM IGNITE.BASELINE_NODES").size());

        res = execSql("SELECT CONSISTENT_ID FROM IGNITE.NODES N WHERE NOT EXISTS (SELECT 1 FROM " +
            "IGNITE.BASELINE_NODES B WHERE B.CONSISTENT_ID = N.CONSISTENT_ID)");

        assertEquals(1, res.size());

        assertEquals("node2", res.get(0).get(0));
    }

    /**
     * Gets ignite configuration with persistance enabled.
     */
    private IgniteConfiguration getPdsConfiguration(String consistentId) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024L * 1024L).setPersistenceEnabled(true))
        );

        cfg.setConsistentId(consistentId);

        return cfg;
    }
}
