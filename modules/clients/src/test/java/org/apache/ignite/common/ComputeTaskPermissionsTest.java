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

package org.apache.ignite.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.cache.VerifyBackupPartitionsTask;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.PublicAccessJob;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.compute.ComputePermissionCheckTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.lang.gridfunc.AtomicIntegerFactoryCallable;
import org.apache.ignite.internal.util.lang.gridfunc.RunnableWrapperClosure;
import org.apache.ignite.internal.util.lang.gridfunc.ToStringClosure;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.common.AbstractEventSecurityContextTest.sendRestRequest;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.COMPUTE_JOB_WORKER_INTERRUPT_TIMEOUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXE;
import static org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor.registerExternalSystemTypes;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_KILL;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ComputeTaskPermissionsTest extends AbstractSecurityTest {
    /** */
    private static final IgniteCallable SYSTEM_CALLABLE = new AtomicIntegerFactoryCallable();

    /** */
    private static final IgniteRunnable SYSTEM_RUNNABLE = new RunnableWrapperClosure(null);

    /** */
    private static final IgniteClosure SYSTEM_CLOSURE = new ToStringClosure<>();

    /** */
    private static final ComputeTask SYSTEM_TASK = new VerifyBackupPartitionsTask();

    /** */
    private static final AtomicInteger EXECUTED_TASK_CNTR = new AtomicInteger();

    /** */
    private static final AtomicInteger CANCELLED_TASK_CNTR = new AtomicInteger();

    /** */
    private static final String CACHE = DEFAULT_CACHE_NAME;

    /** */
    private static final int SRV_NODES_CNT = 2;

    /** */
    public static CountDownLatch taskStartedLatch;

    /** */
    public static CountDownLatch taskUnblockedLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        registerExternalSystemTypes(
            SystemRunnable.class,
            PublicAccessSystemTask.class,
            PublicAccessSystemJob.class
        );

        for (int idx = 0; idx < SRV_NODES_CNT; idx++)
            startGrid(idx, false);

        startGrid(SRV_NODES_CNT, true);

        // Terminates jobs immediately after cancel.
        grid(0).context().distributedConfiguration()
            .property(COMPUTE_JOB_WORKER_INTERRUPT_TIMEOUT)
            .propagate(0L);

        grid(0).createCache(CACHE);
    }

    /** */
    private IgniteEx startGrid(int idx, boolean isClient) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        SecurityPermissionSetBuilder permsBuilder = new SecurityPermissionSetBuilder()
            .defaultAllowAll(false)
            .appendTaskPermissions(AllowedCallable.class.getName(), TASK_EXECUTE)
            .appendTaskPermissions(AllowedCallableSecond.class.getName(), TASK_EXECUTE)
            .appendTaskPermissions(AllowedRunnable.class.getName(), TASK_EXECUTE)
            .appendTaskPermissions(AllowedRunnableSecond.class.getName(), TASK_EXECUTE)
            .appendTaskPermissions(AllowedComputeTask.class.getName(), TASK_EXECUTE)
            .appendTaskPermissions(AllowedClosure.class.getName(), TASK_EXECUTE)
            .appendTaskPermissions(CancelAllowedTask.class.getName(), TASK_EXECUTE, TASK_CANCEL)
            .appendTaskPermissions(CancelForbiddenTask.class.getName(), TASK_EXECUTE)
            .appendTaskPermissions(CancelAllowedRunnable.class.getName(), TASK_EXECUTE, TASK_CANCEL)
            .appendTaskPermissions(CancelForbiddenRunnable.class.getName(), TASK_EXECUTE);

        if (!isClient)
            permsBuilder.appendSystemPermissions(CACHE_CREATE, JOIN_AS_SERVER);

        SecurityPermissionSet permissions = permsBuilder.build();

        IgniteConfiguration cfg = getConfiguration(
            login,
            new TestSecurityPluginProvider(
                login,
                "",
                permissions,
                null,
                false,
                new TestSecurityData("ignite-client", permissions),
                new TestSecurityData("grid-client", permissions),
                new TestSecurityData("rest-client", permissions),
                new TestSecurityData("no-permissions-login-0", NO_PERMISSIONS),
                new TestSecurityData("no-permissions-login-1", NO_PERMISSIONS),
                new TestSecurityData(
                    "admin-kill",
                    create().defaultAllowAll(false)
                        .appendSystemPermissions(ADMIN_KILL)
                        .build()
                ),
                new TestSecurityData(
                    "admin-ops",
                    create().defaultAllowAll(false)
                        .appendSystemPermissions(ADMIN_OPS)
                        .build()
                )

            )).setClientMode(isClient);

        cfg.setConnectorConfiguration(new ConnectorConfiguration()
                .setJettyPath("modules/clients/src/test/resources/jetty/rest-jetty.xml"))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration()
                    .setMaxActiveComputeTasksPerConnection(1)));

        return startGrid(cfg);
    }

    /** */
    @Test
    public void testNode() throws Exception {
        for (int initIdx = 0; initIdx <= SRV_NODES_CNT; initIdx++) {
            for (int execIdx = 0; execIdx < SRV_NODES_CNT; execIdx++)
                checkTaskStart(initIdx, execIdx);

            IgniteCompute compute = grid(initIdx).compute();

            checkTaskCancelSucceeded(() -> new TestFutureAdapter<>(compute.broadcastAsync(new CancelAllowedRunnable())));

            checkTaskCancelFailed(
                () -> new TestFutureAdapter<>(compute.broadcastAsync(new CancelForbiddenRunnable())),
                SecurityException.class
            );

            checkTaskCancel(
                () -> new TestFutureAdapter<>(compute.runAsync(Arrays.asList(
                    new CancelAllowedRunnable(),
                    new CancelForbiddenRunnable()))
                ),
                null,
                2,
                SecurityException.class
            );
        }
    }

    /** */
    @Test
    public void testIgniteClient() throws Exception {
        try (IgniteClient cli = startClient("ignite-client")) {
            checkTask(String.class, (t, a) -> cli.compute().execute(t, a));
            checkTask(String.class, (t, a) -> cli.compute().executeAsync(t, a).get());
            checkTask(String.class, (t, a) -> cli.compute().executeAsync2(t, a).get());

            checkTaskCancelSucceeded(() -> cli.compute().executeAsync2(CancelAllowedTask.class.getName(), null));

            checkTaskCancelFailed(
                () -> cli.compute().executeAsync2(CancelForbiddenTask.class.getName(), null),
                ClientAuthorizationException.class
            );
        }
    }

    /** */
    @Test
    public void testPublicAccessSystemTask() {
        try (IgniteClient cli = startClient("admin-ops")) {
            assertCompleted(
                () -> cli.compute().executeAsync2(PublicAccessSystemTask.class.getName(), null).get(),
                SRV_NODES_CNT
            );
        }

        try (IgniteClient cli = startClient("no-permissions-login-0")) {
            assertFailed(() -> cli.compute().executeAsync2(PublicAccessSystemTask.class.getName(), null).get());
        }

        // Internal tasks with public access permissions explicitly specified still can be executed via Private API
        // without permission checks.
        assertCompleted(
            () -> grid(0).context().task().execute(PublicAccessSystemTask.class, null).get(getTestTimeout()),
            SRV_NODES_CNT
        );
    }

    /** */
    @Test
    public void testRestClient() {
        for (Ignite reqRecipient : Ignition.allGrids()) {
            checkTask(String.class, (t, a) -> {
                JsonNode resp = sendRestRequest(
                    EXE,
                    Arrays.asList(
                        "name=" + t,
                        "destId=" + reqRecipient.cluster().localNode().id()
                    ),
                    "rest-client"
                );

                if (0 != resp.get("successStatus").intValue())
                    throw new IgniteException(resp.get("error").textValue());
            });
        }
    }

    /** */
    private void checkTaskStart(int initiator, int executor) {
        checkTask(ComputeTask.class, (t, a) -> grid(initiator).compute().execute(t, a));
        checkTask(ComputeTask.class, (t, a) -> grid(initiator).compute().executeAsync(t, a).get());

        checkTask(Class.class, (t, a) -> grid(initiator).compute().execute(t, a));
        checkTask(Class.class, (t, a) -> grid(initiator).compute().executeAsync(t, a).get());

        checkTask(String.class, (t, a) -> grid(initiator).compute().execute(t, a));
        checkTask(String.class, (t, a) -> grid(initiator).compute().executeAsync(t, a).get());

        checkCallable(t -> compute(initiator, executor).call(t));
        checkCallable(t -> compute(initiator, executor).callAsync(t).get());

        checkCallables(c -> compute(initiator, executor).call(c));
        checkCallables(c -> compute(initiator, executor).callAsync(c).get());

        checkCallables(c -> compute(initiator, executor).call(c, new TestReducer()));
        checkCallables(c -> compute(initiator, executor).callAsync(c, new TestReducer()).get());

        checkRunnable(r -> compute(initiator, executor).run(r));
        checkRunnable(r -> compute(initiator, executor).runAsync(r).get());

        checkRunnables(r -> compute(initiator, executor).run(r));
        checkRunnables(r -> compute(initiator, executor).runAsync(r).get());

        checkRunnable(r -> compute(initiator, executor).broadcast(r));
        checkRunnable(r -> compute(initiator, executor).broadcastAsync(r).get());

        checkCallable(c -> compute(initiator, executor).broadcast(c));
        checkCallable(c -> compute(initiator, executor).broadcastAsync(c).get());

        checkClosure(c -> compute(initiator, executor).broadcast(c, null));
        checkClosure(c -> compute(initiator, executor).broadcastAsync(c, null).get());

        checkClosure(c -> compute(initiator, executor).apply(c, (Object)null));
        checkClosure(c -> compute(initiator, executor).applyAsync(c, (Object)null).get());

        checkClosure(c -> compute(initiator, executor).apply(c, singletonList(null), new TestReducer()));
        checkClosure(c -> compute(initiator, executor).applyAsync(c, singletonList(null), new TestReducer()).get());

        checkRunnable(r -> compute(initiator, executor).affinityRun(CACHE, keyForNode(executor), r));
        checkRunnable(r -> compute(initiator, executor).affinityRunAsync(CACHE, keyForNode(executor), r).get());

        checkRunnable(r -> compute(initiator, executor).affinityRun(singletonList(CACHE), keyForNode(executor), r));
        checkRunnable(r -> compute(initiator, executor).affinityRunAsync(singletonList(CACHE), keyForNode(executor), r).get());

        checkRunnable(r -> compute(initiator, executor).affinityRun(singletonList(CACHE), partitionForNode(executor), r));
        checkRunnable(r -> compute(initiator, executor).affinityRunAsync(singletonList(CACHE), partitionForNode(executor), r).get());

        checkCallable(r -> compute(initiator, executor).affinityCall(CACHE, keyForNode(executor), r));
        checkCallable(r -> compute(initiator, executor).affinityCallAsync(CACHE, keyForNode(executor), r).get());

        checkCallable(r -> compute(initiator, executor).affinityCall(singletonList(CACHE), keyForNode(executor), r));
        checkCallable(r -> compute(initiator, executor).affinityCallAsync(singletonList(CACHE), keyForNode(executor), r).get());

        checkCallable(r -> compute(initiator, executor).affinityCall(singletonList(CACHE), partitionForNode(executor), r));
        checkCallable(r -> compute(initiator, executor).affinityCallAsync(singletonList(CACHE), partitionForNode(executor), r).get());

        checkRunnable(r -> executorService(initiator, executor).submit(r).get());
        checkRunnable(r -> executorService(initiator, executor).submit(r, null).get());
        checkCallable(c -> executorService(initiator, executor).submit(c).get());

        checkCallable(c -> executorService(initiator, executor).invokeAll(singletonList(c)).forEach(ComputePermissionCheckTest::getQuiet));
        checkCallable(c ->
            executorService(initiator, executor).invokeAll(
                    singletonList(c),
                    getTestTimeout(),
                    MILLISECONDS
                ).forEach(ComputePermissionCheckTest::getQuiet)
        );

        checkCallable(c -> executorService(initiator, executor).invokeAny(singletonList(c)));
        checkCallable(c -> executorService(initiator, executor).invokeAny(singletonList(c), getTestTimeout(), MILLISECONDS));
    }

    /** */
    @Test
    public void testSystemTaskCancel() throws Exception {
        SecurityContext initiatorSecCtx = securityContext("no-permissions-login-0");

        SupplierX<Future<?>> starter = () -> {
            try (OperationSecurityContext ignored1 = grid(0).context().security().withContext(initiatorSecCtx)) {
                return new TestFutureAdapter<>(
                    grid(0).context().closure().runAsync(
                        BROADCAST,
                        new SystemRunnable(),
                        options(grid(0).cluster().forServers().nodes())
                    ).publicFuture()
                );
            }
        };

        // System task initiator does not require any permissions to cancel it.
        checkTaskCancel(starter, initiatorSecCtx, SRV_NODES_CNT, null);

        // System task non-initiator require ADMIN_KILL permission granted to cancel it.
        checkTaskCancel(starter, securityContext("admin-kill"), SRV_NODES_CNT, null);

        // System task cannot be cancelled by a non-initiator without ADMIN_KILL permission granted.
        checkTaskCancel(starter, securityContext("no-permissions-login-1"), SRV_NODES_CNT, SecurityException.class);
    }

    /** */
    private void checkTaskCancelSucceeded(SupplierX<Future<?>> taskStarter) throws Exception {
        checkTaskCancel(taskStarter, null, SRV_NODES_CNT, null);
    }

    /** */
    private void checkTaskCancelFailed(SupplierX<Future<?>> taskStarter, Class<? extends Throwable> expE) throws Exception {
        checkTaskCancel(taskStarter, null, SRV_NODES_CNT, expE);
    }

    /** */
    private void checkTaskCancel(
        SupplierX<Future<?>> taskStarter,
        SecurityContext initiator,
        int expTaskCnt,
        @Nullable Class<? extends Throwable> expE
    ) throws Exception {
        taskStartedLatch = new CountDownLatch(expTaskCnt);
        taskUnblockedLatch = new CountDownLatch(1);

        CANCELLED_TASK_CNTR.set(0);
        EXECUTED_TASK_CNTR.set(0);

        try {
            Future<?> fut = taskStarter.get();

            assertTrue(taskStartedLatch.await(getTestTimeout(), MILLISECONDS));

            try (
                OperationSecurityContext ignored = initiator == null
                    ? null
                    : grid(0).context().security().withContext(initiator)
            ) {
                if (expE == null) {
                    fut.cancel(true);

                    assertTrue(fut.isCancelled());

                    assertTrue(waitForCondition(() -> expTaskCnt == CANCELLED_TASK_CNTR.get(), getTestTimeout()));

                    assertEquals(0, EXECUTED_TASK_CNTR.get());
                }
                else {
                    assertThrowsWithCause(() -> fut.cancel(true), expE);

                    assertFalse(fut.isCancelled());

                    taskUnblockedLatch.countDown();

                    assertTrue(waitForCondition(() -> expTaskCnt == EXECUTED_TASK_CNTR.get(), getTestTimeout()));
                }
            }
        }
        finally {
            taskUnblockedLatch.countDown();
        }
    }

    /** */
    private IgniteCompute compute(int initiator, int executor) {
        IgniteEx ignite = grid(initiator);

        return ignite.compute(ignite.cluster().forNodeId(grid(executor).localNode().id()));
    }

    /** */
    private ExecutorService executorService(int initiator, int executor) {
        IgniteEx ignite = grid(initiator);

        return ignite.executorService(ignite.cluster().forNodeId(grid(executor).localNode().id()));
    }

    /** */
    private void checkRunnable(ConsumerX<IgniteRunnable> consumer) {
        assertCompleted(() -> consumer.accept(new AllowedRunnable()), 1);
        assertFailed(() -> consumer.accept(new ForbiddenRunnable()));
        assertFailed(() -> consumer.accept(SYSTEM_RUNNABLE));
    }

    /** */
    private void checkRunnables(ConsumerX<Collection<IgniteRunnable>> consumer) {
        assertCompleted(() -> consumer.accept(Arrays.asList(new AllowedRunnable(), new AllowedRunnableSecond())), 2);
        assertFailed(() -> consumer.accept(Arrays.asList(new AllowedRunnable(), new ForbiddenRunnable())));
        assertFailed(() -> consumer.accept(Arrays.asList(new AllowedRunnable(), SYSTEM_RUNNABLE)));
    }

    /** */
    private void checkCallable(ConsumerX<IgniteCallable<AtomicInteger>> consumer) {
        assertCompleted(() -> consumer.accept(new AllowedCallable()), 1);
        assertFailed(() -> consumer.accept(new ForbiddenCallable()));
        assertFailed(() -> consumer.accept(SYSTEM_CALLABLE));
    }

    /** */
    private void checkCallables(ConsumerX<Collection<IgniteCallable<AtomicInteger>>> consumer) {
        assertCompleted(() -> consumer.accept(Arrays.asList(new AllowedCallable(), new AllowedCallableSecond())), 2);
        assertFailed(() -> consumer.accept(Arrays.asList(new AllowedCallable(), new ForbiddenCallable())));
        assertFailed(() -> consumer.accept(Arrays.asList(new AllowedCallable(), SYSTEM_CALLABLE)));
    }

    /** */
    private void checkClosure(Consumer<IgniteClosure> consumer) {
        assertCompleted(() -> consumer.accept(new AllowedClosure()), 1);
        assertFailed(() -> consumer.accept(new ForbiddenClosure()));
        assertFailed(() -> consumer.accept(SYSTEM_CLOSURE));
    }

    /** */
    private <T> void checkTask(Class<T> cls, BiConsumerX<T, Object> consumer) {
        assertCompleted(() -> consumer.accept(cast(new AllowedComputeTask(), cls), null), SRV_NODES_CNT);
        assertFailed(() -> consumer.accept(cast(new ForbiddenComputeTask(), cls), null));
        assertFailed(() -> consumer.accept(cast(SYSTEM_TASK, cls), null));
    }

    /** */
    private void assertCompleted(RunnableX r, int expCnt) {
        EXECUTED_TASK_CNTR.set(0);

        r.run();

        assertEquals(expCnt, EXECUTED_TASK_CNTR.get());
    }

    /** */
    private void assertFailed(RunnableX r) {
        EXECUTED_TASK_CNTR.set(0);

        try {
            r.run();
        }
        catch (Exception e) {
            if (X.hasCause(e, ClientAuthorizationException.class) || X.hasCause(e, SecurityException.class))
                return;

            if (e instanceof IgniteException) {
                assertTrue(
                    e.getMessage().contains("Authorization failed") ||
                    e.getMessage().contains("Access to Ignite Internal tasks is restricted") ||
                    e.getMessage().contains("Failed to get any task completion")
                );

                return;
            }
        }

        fail();

        assertEquals(0, EXECUTED_TASK_CNTR.get());
    }

    /** */
    public <T> T cast(Object obj, Class<T> cls) {
        if (String.class.equals(cls))
            return (T)obj.getClass().getName();
        else if (Class.class.equals(cls))
            return (T)obj.getClass();
        else if (ComputeTask.class.equals(cls))
            return (T)obj;
        else
            throw new IllegalStateException();
    }

    /** */
    private int keyForNode(int nodeIdx) {
        return keyForNode(grid(0).affinity(CACHE), new AtomicInteger(0), grid(nodeIdx).cluster().localNode());
    }

    /** */
    private int partitionForNode(int nodeIdx) {
        return grid(0).affinity(CACHE).partition(keyForNode(nodeIdx));
    }

    /** */
    private static class AllowedComputeTask extends AbstractTask { }

    /** */
    private static class ForbiddenComputeTask extends AbstractTask { }

    /** */
    private static class AllowedCallable extends AbstractCallable { }

    /** */
    private static class AllowedCallableSecond extends AbstractCallable { }

    /** */
    private static class ForbiddenCallable extends AbstractCallable { }

    /** */
    private static class AllowedRunnable extends AbstractRunnable { }

    /** */
    private static class AllowedRunnableSecond extends AbstractRunnable { }

    /** */
    private static class ForbiddenRunnable extends AbstractRunnable { }

    /** */
    private static class AllowedClosure extends AbstractClosure { }

    /** */
    private static class ForbiddenClosure extends AbstractClosure { }

    /** */
    private static class CancelAllowedTask extends AbstractHangingTask { }

    /** */
    private static class CancelForbiddenTask extends AbstractHangingTask { }

    /** */
    private static class CancelAllowedRunnable extends AbstractHangingRunnable { }

    /** */
    private static class CancelForbiddenRunnable extends AbstractHangingRunnable { }

    /** */
    private static class SystemRunnable extends AbstractHangingRunnable { }

    /** */
    private static class PublicAccessSystemTask extends AbstractTask {
        /** {@inheritDoc} */
        @Override protected ComputeJob job() {
            return new PublicAccessSystemJob();
        }
    }

    /** */
    private abstract static class AbstractRunnable implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            EXECUTED_TASK_CNTR.incrementAndGet();
        }
    }

    /** */
    private abstract static class AbstractCallable implements IgniteCallable<AtomicInteger> {
        /** {@inheritDoc} */
        @Override public AtomicInteger call() throws Exception {
            EXECUTED_TASK_CNTR.incrementAndGet();

            return new AtomicInteger(0);
        }
    }

    /** */
    private abstract static class AbstractClosure implements IgniteClosure<Boolean, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean apply(Boolean o) {
            EXECUTED_TASK_CNTR.incrementAndGet();

            return null;
        }
    }

    /** */
    private abstract static class AbstractHangingTask extends AbstractTask {
        /** {@inheritDoc} */
        @Override protected ComputeJob job() {
            return new HangingJob();
        }
    }

    /** */
    private abstract static class AbstractHangingRunnable implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            new HangingJob().execute();
        }
    }

    /** */
    private abstract static class AbstractTask extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Object arg
        ) throws IgniteException {
            return subgrid.stream().filter(g -> !g.isClient()).collect(Collectors.toMap(ignored -> job(), srv -> srv));
        }

        /** {@inheritDoc} */
        @Override public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }

        /** */
        protected ComputeJob job() {
            return new TestJob();
        }
    }

    /** */
    private static class PublicAccessSystemJob extends TestJob implements PublicAccessJob {
        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            return SecurityPermissionSetBuilder.systemPermissions(ADMIN_OPS);
        }
    }

    /** */
    private static class TestJob implements ComputeJob {
        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            EXECUTED_TASK_CNTR.incrementAndGet();

            return null;
        }
    }

    /** */
    private static class HangingJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() {
            LockSupport.parkNanos(10_000_000);

            taskStartedLatch.countDown();

            try {
                taskUnblockedLatch.await(5_000, MILLISECONDS);
            }
            catch (InterruptedException e) {
                CANCELLED_TASK_CNTR.incrementAndGet();

                Thread.currentThread().interrupt();

                throw new IgniteException(e);
            }

            EXECUTED_TASK_CNTR.incrementAndGet();

            return null;
        }
    }

    /** */
    private static class TestReducer implements IgniteReducer {

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable Object o) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public Object reduce() {
            return null;
        }
    }

    /** */
    private interface SupplierX<T> {
        /** */
        T get() throws Exception;
    }

    /** */
    private interface BiConsumerX<T, U> {
        /** */
        void accept(T t, U u) throws Exception;
    }

    /** */
    private SecurityContext securityContext(String login) throws Exception {
        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.credentials(new SecurityCredentials(login, ""));
        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(UUID.randomUUID());

        return grid(0).context().security().authenticate(authCtx);
    }

    /** */
    private IgniteClient startClient(String login) {
        return Ignition.startClient(new ClientConfiguration()
            .setUserName(login)
            .setUserPassword("")
            .setAddresses("127.0.0.1:10800"));
    }
}
