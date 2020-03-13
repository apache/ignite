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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest.execute;
import static org.apache.ignite.util.CancelCommandTests.PAGE_SZ;
import static org.apache.ignite.util.CancelCommandTests.doTestCancelComputeTask;
import static org.apache.ignite.util.CancelCommandTests.doTestCancelContinuousQuery;
import static org.apache.ignite.util.CancelCommandTests.doTestCancelSQLQuery;
import static org.apache.ignite.util.CancelCommandTests.doTestCancelService;
import static org.apache.ignite.util.CancelCommandTests.doTestCancelTx;
import static org.apache.ignite.util.CancelCommandTests.doTestScanQueryCancel;

/** Tests cancel of user created entities via SQL. */
public class CancelCommandSQLTest extends GridCommonAbstractTest {
    /** */
    public static final int  NODES_CNT = 3;

    /** */
    public static final String KILL_SQL_QRY = "KILL QUERY ?";

    /** */
    public static final String KILL_SCAN_QRY = "KILL SCAN_QUERY ?, ?, ?";

    /** */
    public static final String KILL_TX_QRY = "KILL TX ?";

    /** */
    public static final String KILL_CQ_QRY = "KILL CONTINUOUS_QUERY ?";

    /** */
    public static final String KILL_COMPUTE_QRY = "KILL COMPUTE_TASK ?";

    /** */
    public static final String KILL_SVC_QRY = "KILL SERVICE ?";

    /** */
    private static List<IgniteEx> srvs;

    /** */
    private static IgniteEx cli;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);

        srvs = new ArrayList<>();

        for (int i = 0; i < NODES_CNT; i++)
            srvs.add(grid(i));

        cli = startClientGrid("client");

        srvs.get(0).cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = cli.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Integer.class)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelScanQuery() throws Exception {
        doTestScanQueryCancel(cli, srvs, args -> execute(cli, KILL_SCAN_QRY, args.get1(), args.get2(), args.get3()));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelSQLQuery() throws Exception {
        doTestCancelSQLQuery(cli, qryId -> execute(cli, KILL_SQL_QRY, qryId));

    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelTx() throws Exception {
        doTestCancelTx(cli, srvs, xid -> execute(cli, KILL_TX_QRY, xid));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelContinuousQuery() throws Exception {
        doTestCancelContinuousQuery(cli, srvs, routineId -> execute(cli, KILL_CQ_QRY, routineId.toString()));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelComputeTask() throws Exception {
        doTestCancelComputeTask(cli, srvs, sessId -> execute(cli, KILL_COMPUTE_QRY, sessId));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        doTestCancelService(cli, srvs.get(0), name -> execute(cli, KILL_SVC_QRY, name));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownSQLQuery() throws Exception {
        execute(cli, KILL_SQL_QRY, srvs.get(0).localNode().id().toString() + "_42'");
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownScanQuery() throws Exception {
        execute(cli, KILL_SCAN_QRY, cli.localNode().id(), "unknown", 1L);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownTx() throws Exception {
        execute(cli, KILL_TX_QRY, "unknown");
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownContinuousQuery() throws Exception {
        execute(cli, KILL_CQ_QRY,  UUID.randomUUID().toString());
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownComputeTask() throws Exception {
        execute(cli, KILL_COMPUTE_QRY, UUID.randomUUID().toString());
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownService() throws Exception {
        execute(cli, KILL_SVC_QRY, "unknown");
    }
}
