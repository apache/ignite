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

package org.apache.ignite.internal.processors.security.service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.security.service.ServiceLifecycleSecurityContextTest.TestService.CANCEL_INVOKE_CNT;
import static org.apache.ignite.internal.processors.security.service.ServiceLifecycleSecurityContextTest.TestService.EXEC_INVOKE_CNT;
import static org.apache.ignite.internal.processors.security.service.ServiceLifecycleSecurityContextTest.TestService.INIT_INVOKE_CNT;
import static org.apache.ignite.internal.processors.security.service.ServiceLifecycleSecurityContextTest.TestService.SRVC_ERR;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_DEPLOY;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_INVOKE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ServiceLifecycleSecurityContextTest extends AbstractSecurityTest {
    /** */
    private static final String SRVC_NAME = "test-service";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        SRVC_ERR.set(null);
        INIT_INVOKE_CNT.set(0);
        EXEC_INVOKE_CNT.set(0);
        CANCEL_INVOKE_CNT.set(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testStaticallyConfiguredService() throws Exception {
        startGrid(staticServiceServerConfiguration(0));
        startGrid(staticServiceServerConfiguration(1));

        try (IgniteClient admin = startThinClient("admin")) {
            admin.cluster().state(ACTIVE);

            validateServiceStartSecurityContext(2);

            startGrid(staticServiceServerConfiguration(2));

            validateServiceStartSecurityContext(3);

            admin.cluster().state(INACTIVE);

            validateServiceCancelSecurityContext(3);
        }
    }

    /** */
    @Test
    public void testDynamicServiceRedeployedAfterDeployerLeft() throws Exception {
        startGrid(serverConfiguration(0));

        IgniteEx canceller = startGrid(nodeConfiguration(
            1,
            "canceller",
            create().defaultAllowAll(false).appendServicePermissions(SRVC_NAME, SERVICE_CANCEL).build(),
            true
        ));

        IgniteEx deployer = startGrid(nodeConfiguration(
            2,
            "deployer",
            create().defaultAllowAll(false).appendServicePermissions(SRVC_NAME, SERVICE_DEPLOY).build(),
            true
        ));

        deployer.services().deploy(serviceConfiguration());

        validateServiceStartSecurityContext(1);

        stopGrid(2);

        startGrid(serverConfiguration(3));

        validateServiceStartSecurityContext(2);

        canceller.services().cancel(SRVC_NAME);

        validateServiceCancelSecurityContext(2);
    }

    /** */
    @Test
    public void testServiceStaticallyConfiguredOnClient() throws Exception {
        startGrid(serverConfiguration(0));

        IgniteConfiguration cliCfg = nodeConfiguration(
            1,
            "client",
            create().defaultAllowAll(false).appendServicePermissions(SRVC_NAME, SERVICE_DEPLOY, SERVICE_CANCEL).build(),
            true
        );

        IgniteEx cli = startGrid(cliCfg.setServiceConfiguration(serviceConfiguration()));

        validateServiceStartSecurityContext(1);

        startGrid(serverConfiguration(2));

        validateServiceStartSecurityContext(2);

        cli.services().cancel(SRVC_NAME);

        validateServiceCancelSecurityContext(2);
    }

    /** */
    @Test
    public void testServiceMethodRunsAsInvoker() throws Exception {
        IgniteConfiguration srvCfg = nodeConfiguration(
            0,
            getTestIgniteInstanceName(0),
            create()
                .defaultAllowAll(false)
                .appendSystemPermissions(JOIN_AS_SERVER, CACHE_CREATE)
                .appendServicePermissions(SRVC_NAME, SERVICE_INVOKE)
                .build(),
            false,
            new TestSecurityData("thin-client", create().defaultAllowAll(false).appendServicePermissions(SRVC_NAME, SERVICE_INVOKE).build())
        );

        IgniteEx srv = startGrid(srvCfg);

        IgniteEx deployer = startGrid(nodeConfiguration(
            1,
            "deployer",
            create().defaultAllowAll(false).appendServicePermissions(SRVC_NAME, SERVICE_DEPLOY, SERVICE_CANCEL).build(),
            true
        ));

        deployer.services().deploy(serviceConfiguration());

        validateServiceStartSecurityContext(1);

        IgniteEx invoker = startGrid(nodeConfiguration(
            2,
            "invoker",
            create().defaultAllowAll(false).appendServicePermissions(SRVC_NAME, SERVICE_INVOKE).build(),
            true
        ));

        assertEquals("invoker", invoker.services().serviceProxy(SRVC_NAME, CallerLoginEchoService.class, false).login());

        assertEquals(srv.name(), srv.services().serviceProxy(SRVC_NAME, CallerLoginEchoService.class, false).login());

        try (IgniteClient thin = startThinClient("thin-client")) {
            assertEquals("thin-client", thin.services().serviceProxy(SRVC_NAME, CallerLoginEchoService.class).login());
        }

        deployer.services().cancel(SRVC_NAME);

        validateServiceCancelSecurityContext(1);
    }

    /** */
    private void validateServiceStartSecurityContext(int expCnt) throws Exception {
        validateServiceCallSecurityContext(INIT_INVOKE_CNT, expCnt);
        validateServiceCallSecurityContext(EXEC_INVOKE_CNT, expCnt);
    }

    /** */
    private void validateServiceCancelSecurityContext(int expCnt) throws Exception {
        validateServiceCallSecurityContext(CANCEL_INVOKE_CNT, expCnt);
    }

    /** */
    private void validateServiceCallSecurityContext(AtomicInteger invocationCnt, int expCnt) throws Exception {
        assertTrue(waitForCondition(() -> invocationCnt.get() == expCnt, getTestTimeout()));

        // TestService records the first service method that runs as a user other than its local node.
        assertNull(SRVC_ERR.get());
    }

    /** */
    private static IgniteClient startThinClient(String login) {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName(login)
            .setUserPassword(""));
    }

    /** */
    private IgniteConfiguration staticServiceServerConfiguration(int nodeIdx) throws Exception {
        IgniteConfiguration cfg = nodeConfiguration(
            nodeIdx,
            getTestIgniteInstanceName(nodeIdx),
            create()
                .defaultAllowAll(false)
                .appendSystemPermissions(JOIN_AS_SERVER, CACHE_CREATE)
                .appendServicePermissions(SRVC_NAME, SERVICE_DEPLOY)
                .build(),
            false,
            new TestSecurityData("admin", systemPermissions(ADMIN_CLUSTER_STATE))
        );

        return cfg
            .setClusterStateOnStart(INACTIVE)
            .setServiceConfiguration(serviceConfiguration());
    }

    /** */
    private IgniteConfiguration serverConfiguration(int nodeIdx) throws Exception {
        return nodeConfiguration(
            nodeIdx,
            getTestIgniteInstanceName(nodeIdx),
            systemPermissions(JOIN_AS_SERVER, CACHE_CREATE),
            false
        );
    }

    /** */
    private IgniteConfiguration nodeConfiguration(
        int nodeIdx,
        String login,
        SecurityPermissionSet perms,
        boolean isClient,
        TestSecurityData... clientData
    ) throws Exception {
        return getConfiguration(
            getTestIgniteInstanceName(nodeIdx),
            new TestSecurityPluginProvider(login, "", perms, globalAuth, clientData)
        ).setClientMode(isClient);
    }

    /** */
    private static ServiceConfiguration serviceConfiguration() {
        return new ServiceConfiguration()
            .setName(SRVC_NAME)
            .setMaxPerNodeCount(1)
            .setService(new TestService());
    }

    /** */
    public static class TestService implements CallerLoginEchoService {
        /** */
        static final AtomicReference<String> SRVC_ERR = new AtomicReference<>();

        /** */
        static final AtomicInteger INIT_INVOKE_CNT = new AtomicInteger();

        /** */
        static final AtomicInteger EXEC_INVOKE_CNT = new AtomicInteger();

        /** */
        static final AtomicInteger CANCEL_INVOKE_CNT = new AtomicInteger();

        /** */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public void init() {
            checkLocalNodeLogin();

            ignite.getOrCreateCache("cache-" + ignite.localNode().id());

            INIT_INVOKE_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            checkLocalNodeLogin();

            EXEC_INVOKE_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            checkLocalNodeLogin();

            CANCEL_INVOKE_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public String login() {
            return ignite.context().security().securityContext().subject().login().toString();
        }

        /** */
        private void checkLocalNodeLogin() {
            String login = login();

            if (!ignite.name().equals(login))
                SRVC_ERR.compareAndSet(null, "Unexpected service method login [exp=" + ignite.name() + ", actual=" + login + ']');
        }
    }

    /** */
    public interface CallerLoginEchoService extends Service {
        /** */
        public String login();
    }
}
