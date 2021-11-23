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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceCallContext;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_DEPLOY;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_INVOKE;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.LogListener.matches;

/** */
@RunWith(Parameterized.class)
public class ServiceAuthorizationTest extends AbstractSecurityTest {
    /** */
    private static final String TEST_SERVICE_NAME = "test-service-name";

    /** */
    private static final ServiceCallContext SERVICE_CALL_CTX = ServiceCallContext.builder()
        .put("key", "val")
        .build();

    /** */
    private static final String DEPLOYMENT_AUTHORIZATION_FAILED_ERR =
        "Authorization failed [perm=SERVICE_DEPLOY, name=" + TEST_SERVICE_NAME;

    /** */
    private static final int ALLOWED_NODE_IDX = 1;

    /** */
    private static final int FORBIDDEN_NODE_IDX = 2;

    /** */
    private static ListeningTestLogger listeningLog;

    /** */
    @Parameterized.Parameter()
    public boolean isClient;

    /** */
    @Parameterized.Parameters(name = "isClient={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[] {true}, new Object[] {false});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(
        String instanceName,
        AbstractTestSecurityPluginProvider pluginProv
    ) throws Exception {
        return super.getConfiguration(instanceName, pluginProv)
            .setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        listeningLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridAllowAll(getTestIgniteInstanceName(0));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testServiceCancel() throws Exception {
        startGrid(configuration(ALLOWED_NODE_IDX, SERVICE_DEPLOY, SERVICE_INVOKE, SERVICE_CANCEL));
        startGrid(configuration(FORBIDDEN_NODE_IDX, SERVICE_DEPLOY, SERVICE_INVOKE));

        checkCancel(srvcs -> srvcs.cancel(TEST_SERVICE_NAME));
        checkCancel(srvcs -> srvcs.cancelAsync(TEST_SERVICE_NAME).get());

        checkCancel(srvcs -> srvcs.cancelAll(Collections.singleton(TEST_SERVICE_NAME)));
        checkCancel(srvcs -> srvcs.cancelAllAsync(Collections.singleton(TEST_SERVICE_NAME)).get());

        checkCancel(IgniteServices::cancelAll);
        checkCancel(srvcs -> srvcs.cancelAllAsync().get());
    }

    /** */
    @Test
    public void testServiceInvoke() throws Exception {
        startGrid(configuration(ALLOWED_NODE_IDX, SERVICE_DEPLOY, SERVICE_INVOKE));
        startGrid(configuration(FORBIDDEN_NODE_IDX, SERVICE_DEPLOY));

        grid(ALLOWED_NODE_IDX).services().deploy(serviceConfiguration());

        if (!isClient) {
            checkInvoke(srvcs -> srvcs.service(TEST_SERVICE_NAME), false);
            checkInvoke(srvcs -> srvcs.services(TEST_SERVICE_NAME), false);
        }

        checkInvoke(srvcs -> srvcs.serviceProxy(TEST_SERVICE_NAME, Service.class, false), true);
        checkInvoke(srvcs -> srvcs.serviceProxy(TEST_SERVICE_NAME, Service.class, false, getTestTimeout()), true);
        checkInvoke(srvcs -> srvcs.serviceProxy(TEST_SERVICE_NAME, Service.class, false, SERVICE_CALL_CTX), true);
        checkInvoke(srvcs ->
            srvcs.serviceProxy(TEST_SERVICE_NAME, Service.class, false, SERVICE_CALL_CTX, getTestTimeout()),
            true
        );
    }

    /** */
    @Test
    public void testServiceDeploy() throws Exception {
        startGrid(configuration(ALLOWED_NODE_IDX, SERVICE_DEPLOY, SERVICE_INVOKE, SERVICE_CANCEL));
        startGrid(configuration(FORBIDDEN_NODE_IDX, SERVICE_INVOKE));

        checkDeploy(srvcs -> srvcs.deploy(serviceConfiguration()), false);
        checkDeploy(srvcs -> srvcs.deployAsync(serviceConfiguration()).get(), false);

        checkDeploy(srvcs -> srvcs.deployAll(Collections.singleton(serviceConfiguration())), false);
        checkDeploy(srvcs -> srvcs.deployAllAsync(Collections.singleton(serviceConfiguration())).get(), false);

        checkDeploy(srvcs -> srvcs.deployMultiple(TEST_SERVICE_NAME, new TestService(), isClient ? 1 : 3, 1), false);
        checkDeploy(srvcs ->
            srvcs.deployMultipleAsync(TEST_SERVICE_NAME, new TestService(), isClient ? 1 : 3, 1).get(),
            false
        );

        checkDeploy(srvcs -> srvcs.deployNodeSingleton(TEST_SERVICE_NAME, new TestService()), false);
        checkDeploy(srvcs -> srvcs.deployNodeSingletonAsync(TEST_SERVICE_NAME, new TestService()).get(), false);

        checkDeploy(srvcs -> srvcs.deployClusterSingleton(TEST_SERVICE_NAME, new TestService()), true);
        checkDeploy(srvcs -> srvcs.deployClusterSingletonAsync(TEST_SERVICE_NAME, new TestService()).get(), true);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        int key = keyForNode(grid(0).affinity(DEFAULT_CACHE_NAME), new AtomicInteger(0), grid(0).cluster().localNode());

        checkDeploy(srvcs ->
            srvcs.deployKeyAffinitySingleton(TEST_SERVICE_NAME, new TestService(), DEFAULT_CACHE_NAME, key),
            true
        );

        checkDeploy(srvcs ->
            srvcs.deployKeyAffinitySingletonAsync(TEST_SERVICE_NAME, new TestService(), DEFAULT_CACHE_NAME, key).get(),
            true
        );
    }

    /** */
    @Test
    public void testPreconfiguredServiceDeployment() throws Exception {
        startClientAllowAll(getTestIgniteInstanceName(1));

        LogListener srvcDeploymentFailedLogLsnr = matches(DEPLOYMENT_AUTHORIZATION_FAILED_ERR).times(2).build();

        listeningLog.registerListener(srvcDeploymentFailedLogLsnr);

        startGrid(configuration(2, SERVICE_INVOKE).setServiceConfiguration(serviceConfiguration()));

        srvcDeploymentFailedLogLsnr.check(getTestTimeout());

        checkServiceOnAllNodes(TEST_SERVICE_NAME, false);

        startGrid(configuration(3, SERVICE_DEPLOY, SERVICE_INVOKE).setServiceConfiguration(serviceConfiguration()));

        waitForCondition(() -> {
            for (Ignite node : G.allGrids()) {
                if (!node.cluster().localNode().isClient() && node.services().service(TEST_SERVICE_NAME) == null)
                    return false;
            }

            return true;
        }, getTestTimeout());

        // Tests preconfigured service deployment on coordinator.
        if (!isClient) {
            stopAllGrids();

            startGrid(configuration(0, SERVICE_DEPLOY, SERVICE_INVOKE).setServiceConfiguration(serviceConfiguration()));

            assertTrue(waitForCondition(() -> grid(0).services().service(TEST_SERVICE_NAME) != null, getTestTimeout()));

            stopGrid(0);

            srvcDeploymentFailedLogLsnr = matches(DEPLOYMENT_AUTHORIZATION_FAILED_ERR).times(1).build();

            listeningLog.registerListener(srvcDeploymentFailedLogLsnr);

            startGrid(configuration(0, SERVICE_INVOKE).setServiceConfiguration(serviceConfiguration()));

            srvcDeploymentFailedLogLsnr.check(getTestTimeout());

            checkServiceOnAllNodes(TEST_SERVICE_NAME, false);
        }
    }
    
    /** */
    private ServiceConfiguration serviceConfiguration() {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setName(TEST_SERVICE_NAME);
        srvcCfg.setService(new TestService());
        
        return srvcCfg;
    }

    /** */
    private IgniteConfiguration configuration(int idx, SecurityPermission... perms) throws Exception {
        String name = getTestIgniteInstanceName(idx);

        return getConfiguration(
            name,
            new TestSecurityPluginProvider(
                name,
                "",
                create()
                    .defaultAllowAll(false)
                    .appendSystemPermissions(JOIN_AS_SERVER)
                    .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_CREATE)
                    .appendTaskPermissions(
                        "org.apache.ignite.internal.processors.affinity.GridAffinityUtils$AffinityJob",
                        TASK_EXECUTE, TASK_CANCEL)
                    .appendServicePermissions(TEST_SERVICE_NAME, perms)
                    .build(),
                null,
                false
            )
        ).setClientMode(isClient);
    }

    /** */
    private void checkServiceOnAllNodes(String name, boolean deployed) {
        for (Ignite node : G.allGrids()) {
            if (!node.cluster().localNode().isClient()) {
                Object srvc = node.services().service(name);

                if (deployed)
                    assertNotNull(srvc);
                else
                    assertNull(srvc);
            }
        }
    }

    /** */
    private void checkFailed(SecurityPermission perm, RunnableX r) {
        if (perm == SERVICE_DEPLOY) {
            Throwable e = assertThrowsWithCause(r, ServiceDeploymentException.class);

            assertEquals(1, X.getSuppressedList(e).stream()
                .filter(t ->
                    t.getMessage().contains("Authorization failed [perm=SERVICE_DEPLOY, name=" + TEST_SERVICE_NAME))
                .count());
        }
        else {
            assertThrowsAnyCause(
                log,
                () -> {
                    r.run();

                    return null;
                },
                SecurityException.class,
                "Authorization failed [perm=" + perm + ", name=" + TEST_SERVICE_NAME
            );
        }
    }

    /** */
    private void checkCancel(Consumer<IgniteServices> c) {
        grid(ALLOWED_NODE_IDX).services().deploy(serviceConfiguration());

        checkServiceOnAllNodes(TEST_SERVICE_NAME, true);

        checkFailed(SERVICE_CANCEL, () -> c.accept(grid(FORBIDDEN_NODE_IDX).services()));

        checkServiceOnAllNodes(TEST_SERVICE_NAME, true);

        c.accept(grid(ALLOWED_NODE_IDX).services());

        checkServiceOnAllNodes(TEST_SERVICE_NAME, false);
    }

    /** */
    private void checkInvoke(Function<IgniteServices, Object> f, boolean isProxy) throws Exception {
        checkFailed(SERVICE_INVOKE, () -> f.apply(grid(FORBIDDEN_NODE_IDX).services()));

        Object res = f.apply(grid(ALLOWED_NODE_IDX).services());

        if (!isProxy) {
            if (res instanceof Collection)
                assertFalse(((Collection<TestService>)res).isEmpty());
            else
                assertTrue((res instanceof TestService));
        }
        else
            ((Service)res).execute();
    }

    /** */
    private void checkDeploy(Consumer<IgniteServices> c, boolean singleton) {
        grid(ALLOWED_NODE_IDX).services().cancel(TEST_SERVICE_NAME);

        checkServiceOnAllNodes(TEST_SERVICE_NAME, false);

        checkFailed(SERVICE_DEPLOY, () -> c.accept(grid(FORBIDDEN_NODE_IDX).services()));

        checkServiceOnAllNodes(TEST_SERVICE_NAME, false);

        c.accept(grid(ALLOWED_NODE_IDX).services());

        if (!singleton)
            checkServiceOnAllNodes(TEST_SERVICE_NAME, true);
        else
            assertTrue(G.allGrids().stream().anyMatch(ignite -> ignite.services().service(TEST_SERVICE_NAME) != null));
    }
    
    /** */
    public static class TestService implements Service {
        // No-op.
    }
}
