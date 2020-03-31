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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.internal.ServiceMXBeanImpl;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.util.KillCommandsTests.PAGE_SZ;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelComputeTask;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelSQLQuery;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelService;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelTx;
import static org.apache.ignite.util.KillCommandsTests.doTestScanQueryCancel;

/** Tests cancel of user created entities via JMX. */
public class KillCommandsMXBeanTest extends GridCommonAbstractTest {
    /** */
    public static final int  NODES_CNT = 3;

    /** */
    private static List<IgniteEx> srvs;

    /** Client that starts task. */
    private static IgniteEx startCli;

    /** Client that kill task. */
    private static IgniteEx killCli;

    /** */
    private static QueryMXBean qryMBean;

    /** */
    private static TransactionsMXBean txMBean;

    /** */
    private static ComputeMXBean computeMBean;

    /** */
    private static ServiceMXBean svcMxBean;

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

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        qryMBean = getMxBean(killCli.name(), "Query",
            QueryMXBeanImpl.class.getSimpleName(), QueryMXBean.class);

        txMBean = getMxBean(killCli.name(), "Transactions",
            TransactionsMXBeanImpl.class.getSimpleName(), TransactionsMXBean.class);

        computeMBean = getMxBean(killCli.name(), "Compute",
            ComputeMXBeanImpl.class.getSimpleName(), ComputeMXBean.class);

        svcMxBean = getMxBean(killCli.name(), "Service",
            ServiceMXBeanImpl.class.getSimpleName(), ServiceMXBean.class);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelScanQuery() throws Exception {
        doTestScanQueryCancel(startCli, srvs, args ->
            qryMBean.cancelScan(args.get1().toString(), args.get2(), args.get3()));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelComputeTask() throws Exception {
        doTestCancelComputeTask(startCli, srvs, sessId -> computeMBean.cancel(sessId));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        doTestCancelService(startCli, killCli, srvs.get(0), name -> svcMxBean.cancel(name));
    }

    /** */
    @Test
    public void testCancelTx() {
        doTestCancelTx(startCli, srvs, xid -> txMBean.cancel(xid));
    }

    /** */
    @Test
    public void testCancelSQLQuery() {
        doTestCancelSQLQuery(startCli, qryId -> qryMBean.cancelSQL(qryId));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownScanQuery() throws Exception {
        assertThrowsWithCause(() -> qryMBean.cancelScan(srvs.get(0).localNode().id().toString(), "unknown", 1L),
            RuntimeException.class);

    }

    /** */
    @Test
    public void testCancelUnknownComputeTask() {
        computeMBean.cancel(IgniteUuid.randomUuid().toString());
    }

    /** */
    @Test
    public void testCancelUnknownTx() {
        txMBean.cancel("unknown");
    }

    /** */
    @Test
    public void testCancelUnknownService() {
        svcMxBean.cancel("unknown");
    }

    /** */
    @Test
    public void testCancelUnknownSQLQuery() {
        qryMBean.cancelSQL(srvs.get(0).localNode().id().toString() + "_42");
    }
}
