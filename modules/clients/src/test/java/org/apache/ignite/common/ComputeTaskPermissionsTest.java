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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadataCollectorTask;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.compute.ComputePermissionCheckTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.lang.gridfunc.AtomicIntegerFactoryCallable;
import org.apache.ignite.internal.util.lang.gridfunc.RunnableWrapperClosure;
import org.apache.ignite.internal.util.lang.gridfunc.ToStringClosure;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.common.AbstractEventSecurityContextTest.sendRestRequest;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
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
    private static final ComputeTask SYSTEM_TASK = new SnapshotMetadataCollectorTask();

    /** */
    private static final AtomicInteger EXECUTED_TASK_CNTR = new AtomicInteger();

    /** */
    private static final AtomicInteger CANCELLED_TASK_CNTR = new AtomicInteger();

    /** */
    private static final String CACHE = DEFAULT_CACHE_NAME;

    /** */
    private static final int SRV_NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int idx = 0; idx < SRV_NODES_CNT; idx++)
            startGrid(idx, false);

        startGrid(SRV_NODES_CNT, true);

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
            .appendTaskPermissions(CancelForbiddenTask.class.getName(), TASK_EXECUTE);

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
                new TestSecurityData("rest-client", permissions)
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
    public void testNode() {
        doNodeTest(0, 0);
        doNodeTest(0, 1);
        doNodeTest(2, 1);
    }

    /** */
    @Test
    public void testIgniteClient() throws Exception {
        try (
            IgniteClient cli = Ignition.startClient(new ClientConfiguration()
                .setUserName("ignite-client")
                .setUserPassword("")
                .setAddresses("127.0.0.1:10800"))
        ) {
            checkTask(String.class, (t, a) -> cli.compute().execute(t, a));
            checkTask(String.class, (t, a) -> cli.compute().executeAsync(t, a).get());
            checkTask(String.class, (t, a) -> cli.compute().executeAsync2(t, a).get());

            testTaskCancelFailed(t -> cli.compute().executeAsync2(t, null));
            testTaskCancelSucceeded(t -> cli.compute().executeAsync2(t, null));
        }
    }

    /** */
    @Test
    public void testGridClient() throws Exception {
        GridClientConfiguration cfg = new GridClientConfiguration()
            .setServers(singletonList("127.0.0.1:11211"))
            .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(new SecurityCredentials("grid-client", "")));

        try (GridClient cli = GridClientFactory.start(cfg)) {
            checkTask(String.class, (t, a) -> cli.compute().execute(t, a));
            checkTask(String.class, (t, a) -> cli.compute().executeAsync(t, a).get());
            checkTask(String.class, (t, a) -> cli.compute().affinityExecute(t, CACHE, "key", a));
            checkTask(String.class, (t, a) -> cli.compute().affinityExecuteAsync(t, CACHE, "key", a).get());
        }
    }

    /** */
    @Test
    public void testRestClient() {
        for (Ignite ignite : Ignition.allGrids()) {
            checkTask(String.class, (t, a) -> {
                JsonNode resp = sendRestRequest(
                    EXE,
                    Arrays.asList(
                        "name=" + t,
                        "destId=" + ignite.cluster().localNode().id()
                    ),
                    "rest-client"
                );

                if (0 != resp.get("successStatus").intValue())
                    throw new IgniteException(resp.get("error").textValue());
            });
        }
    }

    /** */
    private void doNodeTest(int initiator, int executor) {
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
    private void testTaskCancelFailed(Function<String, Future<?>> taskStarter) throws Exception {
        AbstractHangingTask.taskStartedLatch = new CountDownLatch(SRV_NODES_CNT);
        AbstractHangingTask.taskUnblockedLatch = new CountDownLatch(1);

        EXECUTED_TASK_CNTR.set(0);

        Future<?> fut = taskStarter.apply(CancelForbiddenTask.class.getName());

        assertTrue(AbstractHangingTask.taskStartedLatch.await(getTestTimeout(), MILLISECONDS));

        GridTestUtils.assertThrowsWithCause(() -> fut.cancel(true), ClientAuthorizationException.class);

        assertFalse(fut.isCancelled());

        AbstractHangingTask.taskUnblockedLatch.countDown();

        assertTrue(waitForCondition(() -> SRV_NODES_CNT == EXECUTED_TASK_CNTR.get(), getTestTimeout()));
    }

    /** */
    private void testTaskCancelSucceeded(Function<String, Future<?>> taskStarter) throws Exception {
        AbstractHangingTask.taskStartedLatch = new CountDownLatch(SRV_NODES_CNT);
        AbstractHangingTask.taskUnblockedLatch = new CountDownLatch(1);

        CANCELLED_TASK_CNTR.set(0);

        Future<?> fut = taskStarter.apply(CancelAllowedTask.class.getName());

        assertTrue(AbstractHangingTask.taskStartedLatch.await(getTestTimeout(), MILLISECONDS));

        assertTrue(fut.cancel(true));

        assertTrue(waitForCondition(() -> SRV_NODES_CNT == CANCELLED_TASK_CNTR.get(), getTestTimeout()));

        AbstractHangingTask.taskUnblockedLatch.countDown();
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
                    e.getMessage().contains("Authorization failed")
                        || e.getMessage().contains("Access to Ignite Internal tasks is restricted")
                        || e.getMessage().contains("Failed to get any task completion")
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
        /** */
        public static CountDownLatch taskStartedLatch;

        /** */
        public static CountDownLatch taskUnblockedLatch;

        /** {@inheritDoc} */
        @Override protected ComputeJob job() {
            return new Job();
        }

        /** */
        private static class Job implements ComputeJob {
            /** {@inheritDoc} */
            @Override public Object execute() {
                LockSupport.parkNanos(10_000_000);

                taskStartedLatch.countDown();

                try {
                    taskUnblockedLatch.await(5_000, MILLISECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException(e);
                }

                EXECUTED_TASK_CNTR.incrementAndGet();

                return null;
            }

            /** {@inheritDoc} */
            @Override public void cancel() {
                CANCELLED_TASK_CNTR.incrementAndGet();
            }
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
    private interface ConsumerX<T> {
        /** */
        void accept(T t) throws Exception;
    }

    /** */
    private interface BiConsumerX<T, U> {
        /** */
        void accept(T t, U u) throws Exception;
    }
}
