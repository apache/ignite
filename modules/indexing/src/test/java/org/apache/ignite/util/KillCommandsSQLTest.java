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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.sql.SqlKeyword.COMPUTE;
import static org.apache.ignite.internal.sql.SqlKeyword.CONTINUOUS;
import static org.apache.ignite.internal.sql.SqlKeyword.KILL;
import static org.apache.ignite.internal.sql.SqlKeyword.QUERY;
import static org.apache.ignite.internal.sql.SqlKeyword.SCAN;
import static org.apache.ignite.internal.sql.SqlKeyword.SERVICE;
import static org.apache.ignite.internal.sql.SqlKeyword.TRANSACTION;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.util.KillCommandsTests.PAGES_CNT;
import static org.apache.ignite.util.KillCommandsTests.PAGE_SZ;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelComputeTask;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelContinuousQuery;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelSQLQuery;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelService;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelTx;
import static org.apache.ignite.util.KillCommandsTests.doTestScanQueryCancel;

/** Tests cancel of user created entities via SQL. */
public class KillCommandsSQLTest extends GridCommonAbstractTest {
    /** */
    public static final int NODES_CNT = 3;

    /** */
    public static final String KILL_SQL_QRY = KILL + " " + QUERY;

    /** */
    public static final String KILL_COMPUTE_QRY = KILL + " " + COMPUTE;

    /** */
    public static final String KILL_SVC_QRY = KILL + " " + SERVICE;

    /** */
    public static final String KILL_TX_QRY = KILL + " " + TRANSACTION;

    /** */
    public static final String KILL_SCAN_QRY = KILL + " " + SCAN;

    /** */
    public static final String KILL_CQ_QRY = KILL + " " + CONTINUOUS;

    /** */
    private static List<IgniteEx> srvs;

    /** Client that starts tasks. */
    private static IgniteEx startCli;

    /** Client that kills tasks. */
    private static IgniteEx killCli;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);

        srvs = new ArrayList<>();

        for (int i = 0; i < NODES_CNT; i++)
            srvs.add(grid(i));

        startCli = startClientGrid("startClient");
        killCli = startClientGrid("killClient");

        srvs.get(0).cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = startCli.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Integer.class)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        // There must be enough cache entries to keep scan query cursor opened.
        // Cursor may be concurrently closed when all the data retrieved.
        for (int i = 0; i < PAGES_CNT * PAGE_SZ; i++)
            cache.put(i, i);
    }

    /** */
    @Test
    public void testCancelScanQuery() {
        doTestScanQueryCancel(startCli, srvs,
            args -> execute(killCli, KILL_SCAN_QRY + " '" + args.get1() + "' '" + args.get2() + "' " + args.get3()));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelComputeTask() throws Exception {
        doTestCancelComputeTask(startCli, srvs, sessId -> execute(killCli, KILL_COMPUTE_QRY + " '" + sessId + "'"));
    }

    /** */
    @Test
    public void testCancelTx() {
        doTestCancelTx(startCli, srvs, xid -> execute(killCli, KILL_TX_QRY + " '" + xid + "'"));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        doTestCancelService(startCli, killCli, srvs.get(0),
            name -> execute(srvs.get(0), KILL_SVC_QRY + " '" + name + "'"));
    }

    /** */
    @Test
    public void testCancelSQLQuery() {
        doTestCancelSQLQuery(startCli, qryId -> execute(killCli, KILL_SQL_QRY + " '" + qryId + "'"));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelContinuousQuery() throws Exception {
        doTestCancelContinuousQuery(startCli, srvs, (nodeId, routineId) ->
            execute(killCli, KILL_CQ_QRY + " '" + nodeId.toString() + "'" + " '" + routineId.toString() + "'"));
    }

    /** */
    @Test
    public void testCancelUnknownScanQuery() {
        execute(startCli, KILL_SCAN_QRY + " '" + killCli.localNode().id() + "' 'unknown' 1");
    }

    /** */
    @Test
    public void testCancelUnknownComputeTask() {
        execute(killCli, KILL_COMPUTE_QRY + " '" + IgniteUuid.randomUuid() + "'");
    }

    /** */
    @Test
    public void testCancelUnknownService() {
        execute(killCli, KILL_SVC_QRY + " 'unknown'");
    }

    /** */
    @Test
    public void testCancelUnknownTx() {
        execute(killCli, KILL_TX_QRY + " 'unknown'");
    }

    /** */
    @Test
    public void testCancelUnknownSQLQuery() {
        assertThrowsWithCause(
            () -> execute(killCli, KILL_SQL_QRY + " '" + srvs.get(0).localNode().id().toString() + "_42'"),
            RuntimeException.class);
    }

    /** */
    @Test
    public void testCancelUnknownContinuousQuery() {
        execute(startCli,
            KILL_CQ_QRY + " '" + srvs.get(0).localNode().id().toString() + "' '" + UUID.randomUUID() + "'");
    }

    /**
     * Execute query on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    static List<List<?>> execute(Ignite node, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args)
            .setSchema("PUBLIC");

        return queryProcessor(node).querySqlFields(qry, true).getAll();
    }
}
