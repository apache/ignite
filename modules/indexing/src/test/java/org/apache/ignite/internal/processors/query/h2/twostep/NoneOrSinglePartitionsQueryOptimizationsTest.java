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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.H2ResultSetIterator;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertNotEquals;

/**
 * Tests that check behaviour if none or only one partiton was extracted during partitioin pruning.
 */
@RunWith(JUnit4.class)
public class NoneOrSinglePartitionsQueryOptimizationsTest extends GridCommonAbstractTest {
    /** Result retrieval timeout. */
    private static final int RES_RETRIEVAL_TIMEOUT = 5_000;

    /** Nodes count. */
    private static final int NODES_COUNT = 2;

    /** Organizations count. */
    private static final int ORG_COUNT = 100;

    /** Organizations cache name. */
    private static final String ORG_CACHE_NAME = "orgBetweenTest";

    /** Organizations cache. */
    private static IgniteCache<Integer, JoinSqlTestHelper.Organization> orgCache;

    /** Client mode. */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /**
     * @return Query entity for Organization.
     */
    private static Collection<QueryEntity> organizationQueryEntity() {
        QueryEntity entity = new QueryEntity(Integer.class, JoinSqlTestHelper.Organization.class);

        entity.setKeyFieldName("ID");
        entity.getFields().put("ID", String.class.getName());

        return Collections.singletonList(entity);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_COUNT - 1, false);

        clientMode = true;

        startGrid(NODES_COUNT);

        orgCache = ignite(NODES_COUNT).getOrCreateCache(
            new CacheConfiguration<Integer, JoinSqlTestHelper.Organization>(ORG_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setQueryEntities(organizationQueryEntity())
        );

        awaitPartitionMapExchange();

        populateDataIntoOrg();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        orgCache = null;

        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test simple query that leads to multiple partitions but doesn't create megre table.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11019") // Fix explain plan for simple query.
    @Test
    public void testQueryWithMultiplePartitions() throws Exception {
        // This query considered to be simple, so merge table won't be created
        // @see org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery.simpleQuery
        runQuery("select * from Organization org where org._KEY = 1 or org._KEY = 2",
            2, false, false);
    }

    /**
     * Test order by query that leads to multiple partitions and creates megre table.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithMultiplePartitionsOrderBy() throws Exception {
        runQuery("select * from Organization org where org._KEY = 1 or org._KEY = 2 order by org._KEY",
            2, true, false);
    }

    /**
     * Test group by query that leads to multiple partitions and creates megre table.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithMultiplePartitionsGroupBy() throws Exception {
        runQuery("select * from Organization org where org._KEY  between 10 and 20  group by org._KEY",
            11, true, false);
    }

    /**
     * Test having query that leads to multiple partitions and creates megre table.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithMultiplePartitionsHaving() throws Exception {
        runQuery("select org.debtCapital, count(*) from Organization org " +
            "group by org.debtCapital having count(*) < 10", ORG_COUNT, true, false);
    }

    /**
     * Test simple query that leads to sinle partition and doesn't create megre table. Map query is expected to be the
     * same as original query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithSinglePartition() throws Exception {
        runQuery("select * from Organization org where org._KEY = 1 order by org._KEY",
            1, false, true);
    }

    /**
     * Test order by query that leads to single partition and doesn't create megre table. Map query is expected to be
     * the same as original query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithSinglePartitionOrderBy() throws Exception {
        runQuery("select * from Organization org where org._KEY = 1 order by org._KEY",
            1, false, true);
    }

    /**
     * Test group by query that leads to multiple partitions and doesn't create megre table. Map query is expected to be
     * the same as original query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithSinglePartitionGroupBy() throws Exception {
        runQuery("select * from Organization org where org._KEY  between 10 and 10 group by org._KEY",
            1, false, true);
    }

    /**
     * Test having query that leads to multiple partitions and doesn't create megre table. Map query is expected to be
     * the same as original query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithSinglePartitionHaving() throws Exception {
        runQuery("select org.debtCapital, count(*) from Organization org where " +
                "org._KEY = 1 group by org.debtCapital having count(*) < 10", 1, false,
            true);
    }

    /**
     * Test query that leads to zero partitions and doesn't produce neither reduce nor map quries.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testQueryWithNonePartititons() throws Exception {
        TestCommunicationSpi commSpi =
            (TestCommunicationSpi)grid(NODES_COUNT).configuration().
                getCommunicationSpi();

        commSpi.resetQueries();

        IgniteInternalFuture res = GridTestUtils.runAsync(() -> orgCache.query(
            new SqlFieldsQuery("select * from Organization org where " +
                "org._KEY = 1 and org._KEY = 2 order by org._KEY")).getAll());

        List<List<?>> rows = (List<List<?>>)res.get(RES_RETRIEVAL_TIMEOUT);

        assertNotNull(rows);

        assertEquals(0, rows.size());

        assertEquals(0, commSpi.mapQueries.size());
    }

    /**
     * Test query that leads to zero partitions and doesn't produce neither reduce nor map quries.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testQueryWithNonePartititonsAndParams() throws Exception {
        TestCommunicationSpi commSpi =
            (TestCommunicationSpi)grid(NODES_COUNT).configuration().
                getCommunicationSpi();

        commSpi.resetQueries();

        IgniteInternalFuture res = GridTestUtils.runAsync(() -> orgCache.query(
            new SqlFieldsQuery("select * from Organization org where org._KEY = ? and org._KEY = ? order by org._KEY").
                setArgs(1, 2)).getAll());

        List<List<?>> rows = (List<List<?>>)res.get(RES_RETRIEVAL_TIMEOUT);

        assertNotNull(rows);

        assertEquals(0, rows.size());

        assertEquals(0, commSpi.mapQueries.size());
    }

    /**
     * Test simple query that leads to sinle partition and doesn't create megre table. Map query is expected to be the
     * same as original query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithSinglePartitionAndParams() throws Exception {
        runQuery("select * from Organization org where org._KEY = ? order by org._KEY",
            1, false, true, 1);
    }

    /**
     * Test simple query that leads to multiple partitions but doesn't create megre table.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11019") // Fix explain plan for simple query.
    @Test
    public void testQueryWithMultiplePartitionsAndParams() throws Exception {
        runQuery("select * from Organization org where org._KEY = ? or org._KEY = ? ",
            2, false, false, 1, 2);
    }

    /**
     * Runs query and checks that given sql query returns expect rows count.
     *
     * @param sqlQry SQL query
     * @param expResCnt Expected result rows count.
     * @param expMergeTbl Flag that signals that merge table is expected to be created.
     * @param expOriginalQry Flag that signals that orignial sql query is expected as map query.
     * @throws Exception If failed.s
     */
    @SuppressWarnings({"ThrowableNotThrown", "unchecked"})
    private void runQuery(String sqlQry, int expResCnt, boolean expMergeTbl, boolean expOriginalQry, Object... args)
        throws Exception {
        TestCommunicationSpi commSpi =
            (TestCommunicationSpi)grid(NODES_COUNT).configuration().
                getCommunicationSpi();

        commSpi.resetQueries();

        IgniteInternalFuture res = GridTestUtils.runAsync(() -> {
            QueryCursor cursor = orgCache.query(new SqlFieldsQuery(sqlQry).setArgs(args));

            Iterable iter = U.field(cursor,"iterExec");

            Iterator innerIter = U.field(iter.iterator(),"iter");

            if (expMergeTbl)
                assertTrue(innerIter instanceof H2ResultSetIterator);
            else
                assertTrue(innerIter instanceof GridMergeIndexIterator);

            List<List<?>> all = new ArrayList<>();

            while (innerIter.hasNext())
                all.add((List)innerIter.next());

            return all;
        });

        List<List<?>> rows = (List<List<?>>)res.get(RES_RETRIEVAL_TIMEOUT);

        assertNotNull(rows);

        assertEquals(expResCnt, rows.size());

        int mapQueriesCnt = commSpi.mapQueries.size();

        if (expOriginalQry) {
            assertEquals(1, mapQueriesCnt);
            assertEquals(sqlQry, commSpi.mapQueries.get(0));
        }
        else {
            for (String mapQry : commSpi.mapQueries)
                assertNotEquals(expOriginalQry, mapQry);
        }

        // Test explain query.
        QueryCursor explainCursor = orgCache.query(new SqlFieldsQuery("explain " + sqlQry).setArgs(args));

        List<List<?>> explainRes = explainCursor.getAll();

        assertEquals(expMergeTbl ? 2 : 1, explainRes.size());

        if (expMergeTbl)
            assertTrue(((String)explainRes.get(1).get(0)).contains(GridSqlQuerySplitter.mergeTableIdentifier(0)));
    }

    /**
     * Populate organization cache with test data.
     */
    private void populateDataIntoOrg() {
        for (int i = 0; i < ORG_COUNT; i++) {
            JoinSqlTestHelper.Organization org = new JoinSqlTestHelper.Organization();

            org.setName("Organization #" + i);

            org.debtCapital(i);

            orgCache.put(i, org);
        }
    }

    /**
     * Test
     *
     * @throws Exception If failed.
     */
    @Test
    // TODO: 23.01.19 comment
    public void testMultipleQueriesWithMixedPartitionsAndParams() throws Exception {
        // TODO: Add test for the following case: complex qry with many map queries, has 2+ parameter placeholders,
        // TODO: execute with 1 partition, then with many partitions.
        runQuery("select * from Organization org where org._KEY = ? or org._KEY = ? order by org._KEY",
            1, false, true, 1, 1);

        runQuery("select * from Organization org where org._KEY = ? or org._KEY = ? order by org._KEY",
            2, true, false, 1, 2);
    }

    /**
     * Test communication SPI.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Map queries. */
        List<String> mapQueries = new CopyOnWriteArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {

            if (((GridIoMessage)msg).message() instanceof GridH2QueryRequest) {
                GridH2QueryRequest gridH2QryReq = (GridH2QueryRequest)((GridIoMessage)msg).message();

                if (gridH2QryReq.queryPartitions() != null) {
                    for (GridCacheSqlQuery qry : gridH2QryReq.queries())
                        mapQueries.add(qry.query());
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * Clear queries list.
         */
        void resetQueries() {
            mapQueries.clear();
        }
    }
}
