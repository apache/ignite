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
package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;

/**
 *
 */
public class IgniteClientFailuresTest extends GridCommonAbstractTest {
    /** */
    private static final String EXCHANGE_WORKER_BLOCKED_MSG = "threadName=exchange-worker, blockedFor=";

    /** */
    private GridStringLogger inMemoryLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);
        else {
            cfg.setClientFailureDetectionTimeout(10_000);

            cfg.setSystemWorkerBlockedTimeout(5_000);

            cfg.setNetworkTimeout(5_000);

            cfg.setGridLogger(inMemoryLog);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test verifies that FailureProcessor doesn't treat tcp-comm-worker thread as blocked when
     * the thread handles situation of failed client node and thus doesn't print full thread dump into logs.
     *
     * @throws Exception If failed.
     */
    public void testNoMessagesFromFailureProcessor() throws Exception {
        GridStringLogger strLog = new GridStringLogger(false, new GridTestLog4jLogger());

        strLog.logLength(1024 * 1024);

        inMemoryLog = strLog;

        IgniteEx srv = startGrid(0);

        inMemoryLog = null;

        Ignite client00 = startGrid("client00");

        client00.getOrCreateCache(new CacheConfiguration<>("cache0"));

        breakClient(client00);

        boolean waitRes = GridTestUtils.waitForCondition(() -> {
            IgniteClusterEx cl = srv.cluster();

            return (cl.topology(cl.topologyVersion()).size() == 1);
        }, 30_000);

        assertTrue(waitRes);

        assertFalse(strLog.toString().contains("name=tcp-comm-worker"));
    }

    /**
     * Test verifies that when client node failed but not yet cleaned up from topology (because {@link IgniteConfiguration#clientFailureDetectionTimeout} has not been reached yet)
     * it doesn't affect new client connected from the same address.
     *
     * @throws Exception If failed.
     */
    public void testFailedClientLeavesTopologyAfterTimeout() throws Exception {
        IgniteEx srv0 = startGrid(0);

        Ignite client00 = startGrid("client00");

        client00.getOrCreateCache(new CacheConfiguration<>("cache0"));

        breakClient(client00);

        final IgniteClusterEx cl = srv0.cluster();

        assertEquals(2, cl.topology(cl.topologyVersion()).size());

        Ignite client01 = startGrid("client01");

        assertEquals(3, cl.topology(cl.topologyVersion()).size());

        boolean waitRes = GridTestUtils.waitForCondition(() -> (cl.topology(cl.topologyVersion()).size() == 2),
            20_000);

        checkCacheOperations(client01.cache("cache0"));

        assertTrue(waitRes);
    }

    /**
     * Test verifies that when some sys thread (on server node) tries to re-establish connection to failed client
     * and exchange-worker gets blocked waiting for it (e.g. to send partitions full map)
     * it is not treated as {@link FailureType#SYSTEM_WORKER_BLOCKED}
     * because this waiting is finite and part of normal operations.
     *
     * @throws Exception If failed.
     */
    public void testExchangeWorkerIsNotTreatedAsBlockedWhenClientNodeFails() throws Exception {
        GridStringLogger strLog = new GridStringLogger(false, new GridTestLog4jLogger());

        strLog.logLength(1024 * 1024);

        inMemoryLog = strLog;

        IgniteEx srv0 = startGrid(0);

        inMemoryLog = null;

        Ignite client00 = startGrid("client00");

        client00.getOrCreateCache(new CacheConfiguration<>("cache0"));

        startGrid(1);

        breakClient(client00);

        final IgniteClusterEx cl = srv0.cluster();

        assertEquals(3, cl.topology(cl.topologyVersion()).size());

        startGrid("client01");

        boolean waitRes = GridTestUtils.waitForCondition(() -> (cl.topology(cl.topologyVersion()).size() == 3),
            20_000);

        assertTrue(waitRes);

        String logRes = strLog.toString();

        assertFalse(logRes.contains(EXCHANGE_WORKER_BLOCKED_MSG));
    }

    /** */
    private void checkCacheOperations(IgniteCache cache) {
        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        for (int i = 0; i < 100; i++)
            assertEquals(i, cache.get(i));
    }

    /** */
    private void breakClient(Ignite client) {
        GridKernalContext ctx = ((IgniteEx)client).context();

        Object discoSpi = ((Object[])GridTestUtils.getFieldValue(ctx.discovery(), GridManagerAdapter.class, "spis"))[0];

        Object commSpi = ((Object[])GridTestUtils.getFieldValue(ctx.io(), GridManagerAdapter.class, "spis"))[0];

        ((TcpCommunicationSpi)commSpi).simulateNodeFailure();

        ((TcpDiscoverySpi)discoSpi).simulateNodeFailure();
    }
}
