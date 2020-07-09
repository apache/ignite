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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class IgniteClientFailuresTest extends GridCommonAbstractTest {
    /** */
    private GridStringLogger inMemoryLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!igniteInstanceName.startsWith("client")) {
            cfg.setClientFailureDetectionTimeout(10_000);

            cfg.setSystemWorkerBlockedTimeout(5_000);

            cfg.setGridLogger(inMemoryLog);
        }

        return cfg;
    }

    /** */
    @Before
    public void setupClientFailuresTest() {
        stopAllGrids();
    }

    /** */
    @After
    public void tearDownClientFailuresTest() {
        stopAllGrids();
    }

    /**
     * Test verifies that FailureProcessor doesn't treat tcp-comm-worker thread as blocked when
     * the thread handles situation of failed client node and thus doesn't print full thread dump into logs.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoMessagesFromFailureProcessor() throws Exception {
        inMemoryLog = new GridStringLogger(false, new GridTestLog4jLogger());

        inMemoryLog.logLength(1024 * 1024);

        IgniteEx srv = startGrid(0);

        IgniteEx client00 = startClientGrid("client00");

        client00.getOrCreateCache(new CacheConfiguration<>("cache0"));

        breakClient(client00);

        boolean waitRes = GridTestUtils.waitForCondition(() -> {
            IgniteClusterEx cl = srv.cluster();

            return (cl.topology(cl.topologyVersion()).size() == 1);
        }, 30_000);

        assertTrue(waitRes);

        assertFalse(inMemoryLog.toString().contains("name=tcp-comm-worker"));
    }

    /**
     * Test verifies that when client node failed but not yet cleaned up from topology (because {@link IgniteConfiguration#clientFailureDetectionTimeout} has not been reached yet)
     * it doesn't affect new client connected from the same address.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailedClientLeavesTopologyAfterTimeout() throws Exception {
        IgniteEx srv0 = startGrid(0);

        IgniteEx client00 = startClientGrid("client00");

        Thread.sleep(5_000);

        client00.getOrCreateCache(new CacheConfiguration<>("cache0"));

        breakClient(client00);

        final IgniteClusterEx cl = srv0.cluster();

        assertEquals(2, cl.topology(cl.topologyVersion()).size());

        IgniteEx client01 = startClientGrid("client01");

        assertEquals(3, cl.topology(cl.topologyVersion()).size());

        boolean waitRes = GridTestUtils.waitForCondition(() -> (cl.topology(cl.topologyVersion()).size() == 2),
            20_000);

        checkCacheOperations(client01.cache("cache0"));

        assertTrue(waitRes);
    }

    /** */
    private void checkCacheOperations(IgniteCache cache) {
        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        for (int i = 0; i < 100; i++)
            assertEquals(i, cache.get(i));
    }

    /** */
    private void breakClient(IgniteEx client) {
        Object discoSpi = ((Object[])GridTestUtils.getFieldValue(client.context().discovery(), GridManagerAdapter.class, "spis"))[0];

        Object commSpi = ((Object[])GridTestUtils.getFieldValue(client.context().io(), GridManagerAdapter.class, "spis"))[0];

        ((TcpCommunicationSpi)commSpi).simulateNodeFailure();

        ((TcpDiscoverySpi)discoSpi).simulateNodeFailure();
    }
}
