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

package org.apache.ignite.internal.processors.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests service invocation if requested service is not deployed on the local node and the service topology is required.
 */
public class GridServiceProxyTopologyInitializationTest extends GridCommonAbstractTest {
    /** Number of the test nodes. */
    private static final int NODES_CNT = 2;

    /** Name of the service that throws exception during initialization. */
    private static final String BROKEN_SRVC = "broken-service";

    /** Name of the decent service. */
    private static final String DECENT_SRVC = "decent-service";

    /** Name of the attribute that shows whether test servise deployment will be skipped on the node. */
    private static final String ATTR_SKIP_DEPLOYMENT = "skip-deployment";

    /** Latch that indicates whether {@link ServiceSingleNodeDeploymentResultBatch} execution should be proceeded. */
    private final CountDownLatch fullMsgUnblockedLatch = new CountDownLatch(1);

    /** Latch that indicated whether {@link ServiceSingleNodeDeploymentResultBatch} was received on the remote node. */
    private final CountDownLatch fullMsgReceivedLatch = new CountDownLatch(1);

    /** Latch that indicated whether {@link ServiceSingleNodeDeploymentResultBatch} was handled on the remote node. */
    private final CountDownLatch fullMsgHandledLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(NODES_CNT - 1).equals(igniteInstanceName)) {
            ((TestTcpDiscoverySpi) cfg.getDiscoverySpi()).discoveryHook(new DiscoveryHook() {
                @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
                    if (customMsg instanceof ServiceClusterDeploymentResultBatch) {
                        fullMsgReceivedLatch.countDown();

                        try {
                            fullMsgUnblockedLatch.await(getTestTimeout(), MILLISECONDS);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override public void afterDiscovery(DiscoveryCustomMessage customMsg) {
                    if (customMsg instanceof ServiceClusterDeploymentResultBatch)
                        fullMsgHandledLatch.countDown();
                }
            });

            cfg.setUserAttributes(Collections.singletonMap(ATTR_SKIP_DEPLOYMENT, true));
        }

        return cfg;
    }

    /**
     * Ignores the test in case the legacy service processor is used.
     */
    @BeforeClass
    public static void checkServiceProcessorType() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests service invocation after its topology is initialized on the local node.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testServiceTopologyInitialized() throws Exception {
        IgniteEx loc = startGrids(NODES_CNT);

        IgniteEx rmt = grid(NODES_CNT - 1);

        assertEquals(1, fullMsgUnblockedLatch.getCount());

        fullMsgUnblockedLatch.countDown();

        assertEquals(1, fullMsgHandledLatch.getCount());

        deployServices(loc);

        assertTrue(fullMsgHandledLatch.await(getTestTimeout(), MILLISECONDS));

        assertThrowsWithCause(
            () -> rmt.services().serviceProxy(BROKEN_SRVC, Invoker.class, false).invoke(),
            IgniteException.class);

        assertTrue(rmt.services().serviceProxy(DECENT_SRVC, Invoker.class, false).invoke());
    }

    /**
     * Tests service invocation before its topology is initialized on the local node.
     *
     * @throws Exception If fails.
     */
    @Test
    @SuppressWarnings("Convert2MethodRef")
    public void testServiceTopologyInitializationDelayed() throws Exception {
        IgniteEx loc = startGrids(NODES_CNT);

        IgniteEx rmt = grid(NODES_CNT - 1);

        assertEquals(1, fullMsgReceivedLatch.getCount());

        deployServices(loc);

        assertTrue(fullMsgReceivedLatch.await(getTestTimeout(), MILLISECONDS));

        IgniteInternalFuture<Boolean> decentSvcFut = runAsync(() ->
            rmt.services().serviceProxy(DECENT_SRVC, Invoker.class, false).invoke());

        IgniteInternalFuture<Boolean> brokenSvcFut = runAsync(() ->
            rmt.services().serviceProxy(BROKEN_SRVC, Invoker.class, false).invoke());

        U.sleep(500);

        assertEquals(1, fullMsgUnblockedLatch.getCount());

        assertFalse(decentSvcFut.isDone());
        assertFalse(brokenSvcFut.isDone());

        fullMsgUnblockedLatch.countDown();

        assertTrue(decentSvcFut.get(getTestTimeout()));

        assertThrowsWithCause(() -> brokenSvcFut.get(getTestTimeout()), IgniteException.class);
    }

    /**
     * Deploys two services. The deployment of one of which fails on all nodes and the other proceeds smoothly.
     *
     * @param ignite Deployment initiator.
     */
    private void deployServices(Ignite ignite) {
        assertThrowsWithCause(
            () -> ignite.services().deployAll(Arrays.asList(
                getServiceConfiguration(BROKEN_SRVC, new TestService(true)),
                getServiceConfiguration(DECENT_SRVC, new TestService(false)))),
            ServiceDeploymentException.class);
    }

    /**
     * @param name Name of the service.
     * @param srvc Instance of the service.
     * @return Service configuration.
     */
    private ServiceConfiguration getServiceConfiguration(String name, Service srvc) {
        return new ServiceConfiguration()
            .setName(name)
            .setMaxPerNodeCount(1)
            .setNodeFilter(node -> node.attribute(ATTR_SKIP_DEPLOYMENT) == null)
            .setService(srvc);
    }

    /**
     * Service that can be forced to fail during initialization.
     */
    public static class TestService implements Invoker {
        /** Wheter exception is thrown during initialisation. */
        private final boolean initBroken;

        /**
         * @param initBroken Wheter exception is thrown during initialisation.
         */
        public TestService(boolean initBroken) {
            this.initBroken = initBroken;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            if (initBroken)
                throw new RuntimeException("Expected service initialization failure.");
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean invoke() {
            return true;
        }
    }

    /** */
    public interface Invoker extends Service {
        /** */
        public boolean invoke();
    }
}
