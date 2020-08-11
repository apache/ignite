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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;

/**
 * Checks that {@link Service} deploy/cancel by {@link IgniteServices} API works fine if cluster in a
 * {@link ClusterState#ACTIVE_READ_ONLY} mode.
 */
public class GridServiceDeployClusterReadOnlyModeTest extends GridCommonAbstractTest {
    /** Service name. */
    private static final String SERVICE_NAME = "test-service";

    /** Nodes count. */
    private static int NODES_CNT = 2;

    /** Service initialize flag. */
    private static final Map<String, Boolean> SERVICE_INIT_FLAGS = new HashMap<>();

    /** Service execute flag. */
    private static final Map<String, Boolean> SERVICE_EXECUTE_FLAGS = new HashMap<>();

    /** Service cancel flag. */
    private static final Map<String, Boolean> SERVICE_CANCEL_FLAGS = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        SERVICE_INIT_FLAGS.clear();
        SERVICE_EXECUTE_FLAGS.clear();
        SERVICE_CANCEL_FLAGS.clear();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testDeployClusterSingletonAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(s -> s.deployClusterSingleton(SERVICE_NAME, new TestService()), true);

        cancelServiceAndCheck(true);
    }

    /** */
    @Test
    public void testDeployClusterSingletonAsyncAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(s -> s.deployClusterSingletonAsync(SERVICE_NAME, new TestService()).get(), true);

        cancelServiceAndCheck(true);
    }

    /** */
    @Test
    public void testDeployNodeSingletonAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(s -> s.deployNodeSingleton(SERVICE_NAME, new TestService()), false);

        cancelServiceAndCheck(false);
    }

    /** */
    @Test
    public void testDeployNodeSingletonAsyncAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(s -> s.deployNodeSingletonAsync(SERVICE_NAME, new TestService()).get(), false);

        cancelServiceAndCheck(false);
    }

    /** */
    @Test
    public void testDeployMultipleAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(
            s -> s.deployMultiple(SERVICE_NAME, new TestService(), NODES_CNT, 1),
            false
        );

        cancelServiceAndCheck(false);
    }

    /** */
    @Test
    public void testDeployMultipleAsyncAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(
            s -> s.deployMultipleAsync(SERVICE_NAME, new TestService(), NODES_CNT, 1).get(),
            false
        );

        cancelServiceAndCheck(false);
    }

    /** */
    @Test
    public void testDeployAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(s -> s.deploy(serviceConfiguration(SERVICE_NAME)), true);

        cancelServiceAndCheck(true);
    }

    /** */
    @Test
    public void testDeployAsyncAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        deployServiceAndCheck(s -> s.deployAsync(serviceConfiguration(SERVICE_NAME)).get(), true);

        cancelServiceAndCheck(true);
    }

    /** */
    @Test
    public void testDeployAllAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        Collection<String> serviceNames = new HashSet<>();

        for (int i = 0; i < 2; i++)
            serviceNames.add(SERVICE_NAME + "_" + i);

        Set<ServiceConfiguration> configs = serviceNames.stream()
            .map(GridServiceDeployClusterReadOnlyModeTest::serviceConfiguration)
            .collect(toSet());

        grid(0).services().deployAll(configs);

        for (String serviceName : serviceNames)
            checkServiceDeployed(serviceName, true);

        grid(0).services().cancelAll(serviceNames);

        for (String serviceName : serviceNames)
            checkServiceCanceled(serviceName, true);
    }

    /** */
    @Test
    public void testDeployAllAsyncAllowed() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        Collection<String> serviceNames = new HashSet<>();

        for (int i = 0; i < 2; i++)
            serviceNames.add(SERVICE_NAME + "_" + i);

        Set<ServiceConfiguration> configs = serviceNames.stream()
            .map(GridServiceDeployClusterReadOnlyModeTest::serviceConfiguration)
            .collect(toSet());

        grid(0).services().deployAllAsync(configs).get();

        for (String serviceName : serviceNames)
            checkServiceDeployed(serviceName, true);

        grid(0).services().cancelAll(serviceNames);

        for (String serviceName : serviceNames)
            checkServiceCanceled(serviceName, true);
    }

    /** */
    @Test
    public void testCancelAllowed() {
        deployServiceAndCheck(services -> services.deploy(serviceConfiguration(SERVICE_NAME)), true);

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        cancelServiceAndCheck(true);
    }

    /** */
    @Test
    public void testCancelAsyncAllowed() {
        deployServiceAndCheck(services -> services.deploy(serviceConfiguration(SERVICE_NAME)), true);

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        grid(0).services().cancelAsync(SERVICE_NAME).get();

        checkServiceCanceled(true);
    }

    /** */
    @Test
    public void testCancelAllAllowed() {
        Collection<String> serviceNames = new HashSet<>();

        for (int i = 0; i < 2; i++)
            serviceNames.add(SERVICE_NAME + "_" + i);

        Set<ServiceConfiguration> configs = serviceNames.stream()
            .map(GridServiceDeployClusterReadOnlyModeTest::serviceConfiguration)
            .collect(toSet());

        grid(0).services().deployAll(configs);

        for (String serviceName : serviceNames)
            checkServiceDeployed(serviceName, true);

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        grid(0).services().cancelAll(serviceNames);

        for (String name : serviceNames)
            checkServiceCanceled(name, true);
    }

    /** */
    @Test
    public void testCancelAllAsyncAllowed() {
        Collection<String> serviceNames = new HashSet<>();

        for (int i = 0; i < 2; i++)
            serviceNames.add(SERVICE_NAME + "_" + i);

        Set<ServiceConfiguration> configs = serviceNames.stream()
            .map(GridServiceDeployClusterReadOnlyModeTest::serviceConfiguration)
            .collect(toSet());

        grid(0).services().deployAll(configs);

        for (String serviceName : serviceNames)
            checkServiceDeployed(serviceName, true);

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        grid(0).services().cancelAllAsync(serviceNames).get();

        for (String name : serviceNames)
            checkServiceCanceled(name, true);
    }

    /** */
    private void deployServiceAndCheck(Consumer<IgniteServices> clo, boolean singleNode) {
        clo.accept(grid(0).services());

        checkServiceDeployed(singleNode);
    }

    /** */
    private void cancelServiceAndCheck(boolean singleNode) {
        grid(0).services().cancel(SERVICE_NAME);

        checkServiceCanceled(singleNode);
    }

    /** */
    private static void checkServiceCanceled(boolean singleNode) {
        checkServiceCanceled(SERVICE_NAME, singleNode);
    }

    /** */
    private static void checkServiceCanceled(String name, boolean singleNode) {
        checkMap(SERVICE_CANCEL_FLAGS, name, singleNode ? 1 : NODES_CNT, true);
    }

    /** */
    private static void checkServiceDeployed(boolean singleNode) {
        checkServiceDeployed(SERVICE_NAME, singleNode);
    }

    /** */
    private static void checkServiceDeployed(String name, boolean singleNode) {
        checkMap(SERVICE_INIT_FLAGS, name, singleNode ? 1 : NODES_CNT, true);
        checkMap(SERVICE_EXECUTE_FLAGS, name, singleNode ? 1 : NODES_CNT, true);
        checkMap(SERVICE_CANCEL_FLAGS, name, singleNode ? 1 : NODES_CNT, false);
    }

    /** */
    private static void checkMap(Map<String, Boolean> map, String prefix, int expectedCount, boolean expectedValue) {
        Collection<String> matchedNames = new HashSet<>();

        for (Map.Entry<String, Boolean> entry : map.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                matchedNames.add(entry.getKey());

                assertEquals(entry.getKey(), expectedValue, entry.getValue().booleanValue());
            }
        }

        assertEquals(matchedNames.toString(), expectedCount, matchedNames.size());
    }

    /** */
    private static ServiceConfiguration serviceConfiguration(String name) {
        return new ServiceConfiguration()
            .setTotalCount(1)
            .setName(name)
            .setService(new TestService());
    }

    /** */
    private static String name(String serviceName, String nodeName) {
        return serviceName + nodeName;
    }

    /**
     *
     */
    private static class TestService implements Service {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            String key = name(ctx.name(), ignite.name());

            assertFalse(key, SERVICE_CANCEL_FLAGS.get(key));

            SERVICE_CANCEL_FLAGS.put(key, true);
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            String key = name(ctx.name(), ignite.name());

            SERVICE_INIT_FLAGS.put(key, true);
            SERVICE_EXECUTE_FLAGS.put(key, false);
            SERVICE_CANCEL_FLAGS.put(key, false);
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            String key = name(ctx.name(), ignite.name());

            assertFalse(key, SERVICE_EXECUTE_FLAGS.get(key));

            SERVICE_EXECUTE_FLAGS.put(key, true);
        }
    }
}
