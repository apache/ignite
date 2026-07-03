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

package org.apache.ignite.spi.discovery.tcp;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Reproducer for a client node losing a discovery event (topology version gap) while its router and
 * the coordinator are sequentially stopped. The gap manifests as {@code AssertionError: lastVer=N, newVer=N+2}
 * in {@code ClientImpl.updateTopologyHistory}.
 */
public class TcpDiscoveryClientTopologyGapTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 5;

    /** */
    private static final int CLIENTS = 4;

    /** */
    private static final int ITERS = 300;

    /** Critical failures captured on nodes, by instance name. */
    private static final Map<String, Throwable> failures = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setLocalPort(48700);

        cfg.setFailureHandler(new AbstractFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failures.put(igniteInstanceName, failureCtx.error());

                return false;
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Mirrors the topology dance of {@code IgniteCachePartitionLossPolicySelfTest} with 5 nodes and
     * {@code stopNodes={1, 0, 2}}: grid(0) is both the clients' router and the coordinator, grid(2)
     * is the next coordinator. Clients must observe every topology version.
     */
    @Test
    public void testClientSeesAllTopologyVersionsOnSequentialServerStop() throws Exception {
        for (int iter = 0; iter < ITERS; iter++) {
            log.info(">>> Iteration: " + iter);

            failures.clear();

            startGrids(SRVS);

            IgniteEx[] clients = new IgniteEx[CLIENTS];

            for (int i = 0; i < CLIENTS; i++)
                clients[i] = startClientGrid(SRVS + i);

            stopGrid(1, true);
            stopGrid(0, true);
            stopGrid(2, true);

            long expTopVer = SRVS + CLIENTS + 3;

            for (IgniteEx client : clients) {
                assertTrue(
                    "Client stuck [name=" + client.name() +
                        ", topVer=" + client.cluster().topologyVersion() +
                        ", expTopVer=" + expTopVer +
                        ", failures=" + failures + ']',
                    GridTestUtils.waitForCondition(
                        () -> client.cluster().topologyVersion() == expTopVer, 15_000));
            }

            assertTrue("Critical failures detected: " + failures, failures.isEmpty());

            stopAllGrids();
        }
    }
}
