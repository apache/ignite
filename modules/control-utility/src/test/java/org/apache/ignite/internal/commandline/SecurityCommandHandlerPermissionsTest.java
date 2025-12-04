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

package org.apache.ignite.internal.commandline;

import java.security.Permissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ServicesTest;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_USER;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_ROLLING_UPGRADE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class SecurityCommandHandlerPermissionsTest extends GridCommandHandlerAbstractTest {
    /** */
    public static final String TEST_NO_PERMISSIONS_LOGIN = "no-permissions-login";

    /** */
    public static final String TEST_LOGIN = "cli-admin-login";

    /** */
    public static final String DEFAULT_PWD = "pwd";

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return F.asList(CLI_CMD_HND);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        persistenceEnable(false);

        injectTestSystemOut();

        TestJob.CANCELLED_JOB_CNT.set(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testClusterTag() throws Exception {
        Ignite ignite = startGrid(0, userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS));

        assertEquals(
            EXIT_CODE_OK,
            execute(enrichWithConnectionArguments(Collections.singleton("--state"), TEST_NO_PERMISSIONS_LOGIN))
        );

        String out = testOut.toString();

        UUID clId = ignite.cluster().id();
        String clTag = ignite.cluster().tag();

        assertTrue(out.contains("Cluster  ID: " + clId));
        assertTrue(out.contains("Cluster tag: " + clTag));
    }

    /** */
    @Test
    public void testCacheScan() throws Exception {
        checkCommandPermissions(asList("--cache", "scan", DEFAULT_CACHE_NAME), cachePermission(CACHE_READ));
    }

    /** */
    @Test
    public void testCacheDestroy() throws Exception {
        checkCommandPermissions(asList("--cache", "destroy", "--caches", DEFAULT_CACHE_NAME), systemPermissions(CACHE_DESTROY));
    }

    /** */
    @Test
    public void testCacheClear() throws Exception {
        checkCommandPermissions(asList("--cache", "clear", "--caches", DEFAULT_CACHE_NAME), cachePermission(CACHE_REMOVE));
    }

    /** */
    @Test
    public void testCacheCreate() throws Exception {
        String ccfgPath = resolveIgnitePath(
            "modules/control-utility/src/test/resources/config/cache/cache-create-correct.xml"
        ).getAbsolutePath();

        checkCommandPermissions(
            asList("--cache", "create", "--springxmlconfig", ccfgPath),
            systemPermissions(CACHE_CREATE)
        );
    }

    /** */
    @Test
    public void testRollingUpgrade() throws Exception {
        IgniteEx ign = startGrid(
            0,
            userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS),
            userData(TEST_LOGIN, systemPermissions(ADMIN_ROLLING_UPGRADE))
        );

        IgniteProductVersion curVer = IgniteProductVersion.fromString(ign.localNode().attribute(ATTR_BUILD_VER));
        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + ".0";

        List<String> cmdArgs = asList("--rolling-upgrade", "enable", targetVerStr);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, executeOnBehalf(cmdArgs, TEST_NO_PERMISSIONS_LOGIN));

        assertFalse(ign.context().rollingUpgrade().enabled());

        assertEquals(EXIT_CODE_OK, executeOnBehalf(cmdArgs, TEST_LOGIN));

        assertTrue(ign.context().rollingUpgrade().enabled());
        assertEquals(IgniteProductVersion.fromString(targetVerStr), ign.context().rollingUpgrade().versions().get2());

        cmdArgs = asList("--rolling-upgrade", "disable");

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, executeOnBehalf(cmdArgs, TEST_NO_PERMISSIONS_LOGIN));

        assertTrue(ign.context().rollingUpgrade().enabled());

        assertEquals(EXIT_CODE_OK, executeOnBehalf(cmdArgs, TEST_LOGIN));

        assertFalse(ign.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testServiceCancel() throws Exception {
        String srvcName = "testService";
        Collection<String> cmdArgs = asList("--kill", "service", srvcName);

        Ignite ignite = startGrid(
            0,
            userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS),
            userData(TEST_LOGIN, servicePermission(srvcName, SERVICE_CANCEL))
        );

        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName(srvcName);
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setTotalCount(1);
        srvcCfg.setService(new ServicesTest.TestService());

        ignite.services().deploy(srvcCfg);

        Collection<ServiceDescriptor> svcs = ignite.services().serviceDescriptors();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, executeOnBehalf(cmdArgs, TEST_NO_PERMISSIONS_LOGIN));
        assertEquals(1, svcs.size());

        assertEquals(EXIT_CODE_OK, executeOnBehalf(cmdArgs, TEST_LOGIN));

        svcs = ignite.services().serviceDescriptors();
        assertEquals(0, svcs.size());
    }

    /** */
    @Test
    public void testTaskCancel() throws Exception {
        IgniteEx ignite = startGrid(
            0,
            userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS),
            userData(TEST_LOGIN, SecurityPermissionSetBuilder.create()
                .defaultAllowAll(false)
                .appendTaskPermissions(TestTask.class.getName(), TASK_CANCEL)
                .build())
        );

        ComputeTaskFuture<ComputeJobResult> fut = ignite.compute().executeAsync(new TestTask(), null);

        GridSystemViewManager viewMgr = ignite.context().systemView();

        assertTrue(waitForCondition(() -> viewMgr.view(TASKS_VIEW).size() >= 1, getTestTimeout()));
        assertTrue(waitForCondition(() -> viewMgr.view(JOBS_VIEW).size() >= 1, getTestTimeout()));

        Iterator<ComputeTaskView> iter = viewMgr.<ComputeTaskView>view(TASKS_VIEW).iterator();

        String sesId = iter.next().sessionId().toString();

        assertFalse(iter.hasNext());

        Collection<String> cmdArgs = asList("--kill", "compute", sesId);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, executeOnBehalf(cmdArgs, TEST_NO_PERMISSIONS_LOGIN));

        assertFalse(fut.isDone());

        assertEquals(0, TestJob.CANCELLED_JOB_CNT.get());

        assertTrue(waitForCondition(() -> viewMgr.view(TASKS_VIEW).size() >= 1, getTestTimeout()));
        assertTrue(waitForCondition(() -> viewMgr.view(JOBS_VIEW).size() >= 1, getTestTimeout()));

        assertEquals(EXIT_CODE_OK, executeOnBehalf(cmdArgs, TEST_LOGIN));

        try {
            assertTrue(fut.get(getTestTimeout(), MILLISECONDS).isCancelled());
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, IgniteFutureCancelledException.class));
        }

        assertTrue(waitForCondition(() -> viewMgr.view(TASKS_VIEW).size() == 0, getTestTimeout()));
        assertTrue(waitForCondition(() -> viewMgr.view(JOBS_VIEW).size() == 0, getTestTimeout()));

        assertEquals(1, TestJob.CANCELLED_JOB_CNT.get());
    }

    /** */
    protected IgniteEx startGrid(int idx, TestSecurityData... userData) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setPluginProviders(new TestSecurityPluginProvider(login, DEFAULT_PWD, ALL_PERMISSIONS, false, userData));

        return startGrid(cfg);
    }

    /** */
    private void checkCommandPermissions(Collection<String> cmdArgs, SecurityPermissionSet reqPerms) throws Exception {
        Ignite ignite = startGrid(
            0,
            userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS),
            userData(TEST_LOGIN, reqPerms)
        );

        ignite.createCache(DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, executeOnBehalf(cmdArgs, TEST_NO_PERMISSIONS_LOGIN));

        // We are losing command failure cause for --cache clear command. See IGNITE-21023 for more details.
        if (!cmdArgs.containsAll(Arrays.asList("--cache", "clear")))
            assertTrue(testOut.toString().contains("Authorization failed"));

        assertEquals(EXIT_CODE_OK, executeOnBehalf(cmdArgs, TEST_LOGIN));
    }

    /** */
    public static List<String> enrichWithConnectionArguments(Collection<String> cmdArgs, String login) {
        List<String> args = new ArrayList<>();

        args.add(CMD_USER);
        args.add(login);
        args.add(CMD_PASSWORD);
        args.add(DEFAULT_PWD);

        args.addAll(cmdArgs);

        return args;
    }

    /** */
    private SecurityPermissionSet cachePermission(SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendCachePermissions(DEFAULT_CACHE_NAME, perms)
            .build();
    }

    /** */
    private SecurityPermissionSet servicePermission(String name, SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendServicePermissions(name, perms)
            .build();
    }

    /** */
    private TestSecurityData userData(String login, SecurityPermissionSet perms) {
        return new TestSecurityData(
            login,
            DEFAULT_PWD,
            perms,
            new Permissions()
        );
    }

    /** */
    private int executeOnBehalf(Collection<String> cmdArgs, String login) {
        return execute(enrichWithConnectionArguments(cmdArgs, login));
    }

    /** */
    private static class TestTask extends ComputeTaskAdapter<Object, ComputeJobResult> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Object arg
        ) throws IgniteException {
            return subgrid.stream().filter(g -> !g.isClient()).collect(Collectors.toMap(ignored -> new TestJob(), srv -> srv));
        }

        /** {@inheritDoc} */
        @Override public @Nullable ComputeJobResult reduce(List<ComputeJobResult> results) throws IgniteException {
            assertTrue(results.size() == 1);

            return results.get(0);
        }
    }

    /** */
    private static class TestJob implements ComputeJob {
        /** */
        private final CountDownLatch jobBlockedLatch = new CountDownLatch(1);

        /** */
        private static final AtomicInteger CANCELLED_JOB_CNT = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void cancel() {
            jobBlockedLatch.countDown();

            CANCELLED_JOB_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            try {
                assertTrue(jobBlockedLatch.await(5_000, MILLISECONDS));
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }
}
