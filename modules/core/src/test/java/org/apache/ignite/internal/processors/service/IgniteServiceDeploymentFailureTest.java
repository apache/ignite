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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test class for verifying the behavior of Ignite service deployment when
 * specified service classes are not found or when service initialization fails.
 */
public class IgniteServiceDeploymentFailureTest extends GridCommonAbstractTest {
    /** */
    private static final String NOOP_SERVICE_CLS_NAME = "org.apache.ignite.tests.p2p.NoopService";

    /** */
    public static final String NODE_FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.ExcludeNodeFilter";

    /** */
    private static final int SERVER_NODES_CNT = 5;

    /** */
    private static final int CLIENT_NODES_CNT = 4;

    /** */
    private static final int TIMEOUT = 10_000;

    /** */
    private static ClassLoader extClsLdr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);
        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        extClsLdr = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        extClsLdr = null;
    }

    /**
     *  Tests that deploying a service with a missing class causes a ServiceDeploymentException.
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testFailWhenClassNotFound() throws Exception {
        IgniteEx srv = startGrid(getConfiguration("server"));
        IgniteEx cli = startClientGrid(1);

        ServiceConfiguration svcCfg = new ServiceConfiguration()
                .setName("TestDeploymentService")
                .setService(((Class<Service>)extClsLdr.loadClass(NOOP_SERVICE_CLS_NAME)).getDeclaredConstructor().newInstance())
                .setNodeFilter(((Class<IgnitePredicate<ClusterNode>>)extClsLdr.loadClass(NODE_FILTER_CLS_NAME))
                        .getConstructor(UUID.class)
                        .newInstance(cli.configuration().getNodeId()))
                .setTotalCount(1);

        assertThrowsWithCause(() -> cli.services().deploy(svcCfg), ServiceDeploymentException.class);

        assertTrue(cli.services().serviceDescriptors().isEmpty());
    }

    /**
     * Service that throws an exception in init.
     * */
    private static class InitThrowingService implements Service {
        /** */
        public static final AtomicInteger initCounter = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            initCounter.incrementAndGet();
            throw new Exception("Service init exception");
        }
    }

    /**
     * NoopService for testing.
     * */
    private static class NoopService implements Service {
        /** */
        public static final AtomicInteger initCounter = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            initCounter.incrementAndGet();
        }
    }

    /**
     * Tests that service descriptors are clear after an attempt of deploying
     * a service which throws an exception during initialization
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testServerDescriptorsOfFailedServices() throws Exception {
        final int NOOP_SRVC_TOTAL_CNT = 20;
        final int NOOP_SRVC_MAX_PER_NODE_CNT_0 = 2;
        final int NOOP_SRVC_MAX_PER_NODE_CNT_1 = 4;

        final int THROW_SRVC_TOTAL_CNT = 10;
        final int THROW_SRVC_MAX_PER_NODE_CNT = 2;

        IgniteEx[] servers = new IgniteEx[SERVER_NODES_CNT];
        for (int i = 0; i < SERVER_NODES_CNT; i++)
            servers[i] = startGrid(getConfiguration("server" + i));

        IgniteEx[] clients = new IgniteEx[CLIENT_NODES_CNT];
        for (int i = 0; i < clients.length; i++)
            clients[i] = startClientGrid(i);

        ServiceConfiguration noopSrvcCfg = new ServiceConfiguration()
                .setName(NoopService.class.getSimpleName())
                .setService(new NoopService())
                .setTotalCount(NOOP_SRVC_TOTAL_CNT)
                .setMaxPerNodeCount(NOOP_SRVC_MAX_PER_NODE_CNT_0);

        ServiceConfiguration throwSrvcCfg = new ServiceConfiguration()
                .setName(InitThrowingService.class.getSimpleName())
                .setService(new InitThrowingService())
                .setTotalCount(THROW_SRVC_TOTAL_CNT)
                .setMaxPerNodeCount(THROW_SRVC_MAX_PER_NODE_CNT);

        clients[0].services().deploy(noopSrvcCfg);
        assertEquals(NOOP_SRVC_MAX_PER_NODE_CNT_0 * SERVER_NODES_CNT,
                getTotalInstancesCount(clients[0], NoopService.class.getSimpleName()));

        assertThrowsWithCause(() -> clients[0].services().deploy(throwSrvcCfg), ServiceDeploymentException.class);

        assertTrue(waitForCondition(() -> {
            boolean noServiceOnServers = Arrays.stream(servers).allMatch(server ->
                    server.services().serviceDescriptors().stream().noneMatch(desc ->
                            desc.name().equals(InitThrowingService.class.getSimpleName())
                    )
            );

            boolean noServiceOnClients = Arrays.stream(clients).allMatch(client ->
                    client.services().serviceDescriptors().stream().noneMatch(desc ->
                            desc.name().equals(InitThrowingService.class.getSimpleName())
                    )
            );

            return noServiceOnServers && noServiceOnClients;
        }, TIMEOUT));

        clients[0].services().cancel(NoopService.class.getSimpleName());

        noopSrvcCfg.setMaxPerNodeCount(NOOP_SRVC_MAX_PER_NODE_CNT_1);
        clients[0].services().deploy(noopSrvcCfg);

        assertEquals(NOOP_SRVC_TOTAL_CNT, getTotalInstancesCount(clients[0], NoopService.class.getSimpleName()));
    }

    /**
     * Retrieves the total instances count of the service.
     *
     * @param igniteEx Ignite instance.
     * @param srvcName Service name.
     * @return Total instances count of the service named {@code srvcName} from the given {@code igniteEx} instance.
     */
    private static int getTotalInstancesCount(IgniteEx igniteEx, String srvcName) {
        Optional<ServiceDescriptor> desc = igniteEx.services().serviceDescriptors().stream().
                filter(descriptor -> descriptor.name().equals(srvcName)).findAny();
        assertTrue(desc.isPresent());
        return desc.get().topologySnapshot().values().stream().mapToInt(Integer::intValue).sum();
    }
}
