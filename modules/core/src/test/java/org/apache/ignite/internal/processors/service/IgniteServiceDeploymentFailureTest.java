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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteServicesImpl;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.*;

/**
 * Test class for verifying the behavior of Ignite service deployment when
 * specified service classes are not found or when service initialization fails.
 */
public class IgniteServiceDeploymentFailureTest extends GridCommonAbstractTest {
    /** */
    private static final String NODE_FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.ExcludeNodeFilter";

    /** */
    private static final String NOOP_SERVICE_NAME = "NoopService";

    /** */
    private static final String INIT_THROWING_SERVICE_NAME = "InitThrowingService";

    /** */
    private static final int SERVER_NODES_CNT = 5;

    /** */
    private static final int CLIENT_NODES_CNT = 4;

    /** */
    private static final int TIMEOUT = 10_000;

    /** */
    private static final String DEPLOYED_SERVICE_MUST_BE_PRESENTED_IN_CLUSTER = "Deployed service must be presented in cluster";

    /** */
    private static final String FAILED_SERVICE_SHOULD_NOT_BE_PRESENT_IN_THE_CLUSTER =
        "Service descriptors whose deployment has failed should not be present in the cluster";

    /** */
    public static final String REGISTERED_SERVICES_BY_NAME = "registeredServicesByName";

    /** */
    public static final String DEPLOYED_SERVICES = "deployedServices";

    /** */
    public static final String DEPLOYED_SERVICES_BY_NAME = "deployedServicesByName";

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
     * Tests that deploying a service with a missing class causes a ServiceDeploymentException.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailWhenClassNotFound() throws Exception {
        startGrid();

        IgniteEx cli = startClientGrid(1);

        ServiceConfiguration svcCfg = new ServiceConfiguration()
            .setName(NOOP_SERVICE_NAME)
            .setService(new NoopService())
            .setNodeFilter(((Class<IgnitePredicate<ClusterNode>>)extClsLdr.loadClass(NODE_FILTER_CLS_NAME))
                .getConstructor(UUID.class)
                .newInstance(cli.configuration().getNodeId()))
            .setTotalCount(1);

        assertThrowsWithCause(() -> cli.services().deploy(svcCfg), ServiceDeploymentException.class);

        assertTrue(waitForCondition(() -> noDescriptorInClusterForService(NOOP_SERVICE_NAME), TIMEOUT));
    }

    /**
     * Tests that service descriptors are clear after an attempt of deploying a service which throws an exception
     * during initialization. Dynamic configuration for services is used.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailedServiceDescriptorsDynamicConfiguration() throws Exception {
        final int maxNoopSrvcMaxPerNodeCnt = 4;

        startGrids(SERVER_NODES_CNT);

        IgniteEx client = startClientGrid(SERVER_NODES_CNT);

        for (int i = 1; i < CLIENT_NODES_CNT; i++)
            startClientGrid(SERVER_NODES_CNT + i);

        ServiceConfiguration noopSrvcCfg = new ServiceConfiguration()
            .setName(NOOP_SERVICE_NAME)
            .setService(new NoopService())
            .setTotalCount(SERVER_NODES_CNT * maxNoopSrvcMaxPerNodeCnt)
            .setMaxPerNodeCount(2);

        // Deploying NoopService - should be completed successfully.
        client.services().deploy(noopSrvcCfg);

        // Check that the expected number of instances has been deployed.
        assertEquals(
            DEPLOYED_SERVICE_MUST_BE_PRESENTED_IN_CLUSTER,
            2 * SERVER_NODES_CNT,
            totalInstancesCount(client, NOOP_SERVICE_NAME)
        );

        // Deploy InitThrowingService that throws an exception in init - should not be deployed.
        assertThrowsWithCause(
            () -> client.services().deploy(new ServiceConfiguration()
                .setName(INIT_THROWING_SERVICE_NAME)
                .setService(new InitThrowingService())
                .setTotalCount(10)
                .setMaxPerNodeCount(2)),
            ServiceDeploymentException.class
        );

        assertTrue(
            FAILED_SERVICE_SHOULD_NOT_BE_PRESENT_IN_THE_CLUSTER,
            waitForCondition(() -> noDescriptorInClusterForService(INIT_THROWING_SERVICE_NAME), TIMEOUT)
        );

        assertEquals(
            DEPLOYED_SERVICE_MUST_BE_PRESENTED_IN_CLUSTER,
            2 * SERVER_NODES_CNT,
            totalInstancesCount(client, NOOP_SERVICE_NAME)
        );

        client.services().cancel(NOOP_SERVICE_NAME);

        // Deploy some additional NoopService instances.
        noopSrvcCfg.setMaxPerNodeCount(maxNoopSrvcMaxPerNodeCnt);

        client.services().deploy(noopSrvcCfg);

        assertEquals(
            DEPLOYED_SERVICE_MUST_BE_PRESENTED_IN_CLUSTER,
            SERVER_NODES_CNT * maxNoopSrvcMaxPerNodeCnt,
            totalInstancesCount(client, NOOP_SERVICE_NAME)
        );
    }

    /**
     * Tests that service descriptors are clear after an attempt of deploying a service which throws an exception
     * during initialization. Static configuration for services is used.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailedServiceDescriptorsStaticConfiguration() throws Exception {
        final int noopSrvcTotalCnt = 20;

        startGrids(SERVER_NODES_CNT - 1);

        for (int i = 0; i < CLIENT_NODES_CNT; i++)
            startClientGrid(SERVER_NODES_CNT + i);

        IgniteEx ign = startGrid(
            getConfiguration().setServiceConfiguration(
                new ServiceConfiguration()
                    .setName(NOOP_SERVICE_NAME)
                    .setService(new NoopService())
                    .setTotalCount(noopSrvcTotalCnt),
                new ServiceConfiguration()
                    .setName(INIT_THROWING_SERVICE_NAME)
                    .setService(new InitThrowingService())
                    .setTotalCount(20)
            )
        );

        assertTrue(
            FAILED_SERVICE_SHOULD_NOT_BE_PRESENT_IN_THE_CLUSTER,
            waitForCondition(() -> noDescriptorInClusterForService(INIT_THROWING_SERVICE_NAME), TIMEOUT)
        );

        assertEquals(
            DEPLOYED_SERVICE_MUST_BE_PRESENTED_IN_CLUSTER,
            noopSrvcTotalCnt,
            totalInstancesCount(ign, NOOP_SERVICE_NAME)
        );
    }

    /**
     * @param srvcName Service name.
     */
    private static boolean noDescriptorInClusterForService(String srvcName) {
        return Ignition.allGrids().stream().allMatch(
            node -> node.services().serviceDescriptors().stream().noneMatch(filterByName(srvcName)) &&
                    getServicesMap(node, REGISTERED_SERVICES_BY_NAME).values().stream().noneMatch(filterByName(srvcName)) &&
                    getServicesMap(node, DEPLOYED_SERVICES).values().stream().noneMatch(filterByName(srvcName)) &&
                    getServicesMap(node, DEPLOYED_SERVICES_BY_NAME).values().stream().noneMatch(filterByName(srvcName))
        );
    }

    /**
     * Retrieves the total instances count of the service.
     *
     * @param igniteEx Ignite instance.
     * @param srvcName Service name.
     * @return Total instances count of the service named {@code srvcName} from the given {@code igniteEx} instance.
     */
    private static int totalInstancesCount(IgniteEx igniteEx, String srvcName) {
        int registeredCnt = totalInstancesCount(igniteEx.services().serviceDescriptors(), srvcName);

        assertEquals(registeredCnt, totalInstancesCount(, srvcName));
        
        return igniteEx.services().serviceDescriptors().stream()
            .filter(filterByName(srvcName))
            .flatMap(desc -> desc.topologySnapshot().values().stream())
            .mapToInt(Integer::intValue).sum();
    }

    /**
     * @param descs Descriptors.
     * @param srvcName Service name.
     * @return Total instances count of the service named {@code srvcName} from the given service descriptors {@code descs}.
     */
    private static <T> int totalInstancesCount(Collection<ServiceDescriptor> descs, String srvcName) {
        return descs.stream()
            .filter(filterByName(srvcName))
            .flatMap(desc -> desc.topologySnapshot().values().stream())
            .mapToInt(Integer::intValue).sum();
    }

    /**
     * Filters descriptors of services with given name.
     *
     * @param srvcName Service name.
     */
    private static @NotNull Predicate<ServiceDescriptor> filterByName(String srvcName) {
        return desc -> desc.name().equals(srvcName);
    }

    /**
     * @param node Node.
     * @param mapName Map name.
     */
    private static <T> Map<T, ServiceInfo> getServicesMap(Ignite node, String mapName) {
        return getFieldValue(getIgniteServiceProcessor(node), IgniteServiceProcessor.class, mapName);
    }

    /**
     * @param node Node.
     */
    private static IgniteServiceProcessor getIgniteServiceProcessor(Ignite node) {
        return ((GridKernalContext)getFieldValue(node.services(), IgniteServicesImpl.class, "ctx")).service();
    }

    /**
     * Service that throws an exception in init.
     */
    private static class InitThrowingService implements Service {
        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            throw new Exception("Service init exception");
        }
    }

    /**
     * NoopService for testing.
     */
    private static class NoopService implements Service {
        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            // No-op.
        }
    }
}
