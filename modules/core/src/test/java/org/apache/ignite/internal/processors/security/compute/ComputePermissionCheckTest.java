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

package org.apache.ignite.internal.processors.security.compute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonList;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Task execute permission tests.
 */
@RunWith(JUnit4.class)
public class ComputePermissionCheckTest extends AbstractSecurityTest {
    /** Flag that shows task was executed. */
    private static final AtomicBoolean IS_EXECUTED = new AtomicBoolean(false);

    /** Reentrant lock. */
    private static final ReentrantLock RNT_LOCK = new ReentrantLock();

    /** Reentrant lock timeout. */
    private static final int RNT_LOCK_TIMEOUT = 20_000;

    /** Test compute task. */
    private static final TestComputeTask TEST_COMPUTE_TASK = new TestComputeTask();

    /** Test callable. */
    private static final IgniteCallable<Object> TEST_CALLABLE = () -> {
        IS_EXECUTED.set(true);

        syncForCancel();

        return null;
    };

    /** Test runnable. */
    private static final IgniteRunnable TEST_RUNNABLE = () -> {
        IS_EXECUTED.set(true);

        syncForCancel();
    };

    /** Test closure. */
    private static final IgniteClosure<Object, Object> TEST_CLOSURE = a -> {
        IS_EXECUTED.set(true);

        syncForCancel();

        return null;
    };

    /** Synchronization for tests TASK_CANCEL. */
    private static void syncForCancel() {
        boolean isLocked = false;

        try {
            isLocked = RNT_LOCK.tryLock(RNT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }
        finally {
            if (isLocked)
                RNT_LOCK.unlock();
        }
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        Ignite srvAllowed = startGrid("srv_allowed", permissions(TASK_EXECUTE, TASK_CANCEL));

        Ignite srvForbidden = startGrid("srv_forbidden", permissions(EMPTY_PERMS));

        Ignite srvForbiddenCancel = startGrid("srv_forbidden_cnl", permissions(TASK_EXECUTE));

        Ignite clntAllowed = startClient("clnt_allowed", permissions(TASK_EXECUTE, TASK_CANCEL));

        Ignite clntForbidden = startClient("clnt_forbidden", permissions(EMPTY_PERMS));

        Ignite clntForbiddenCancel = startClient("clnt_forbidden_cnl", permissions(TASK_EXECUTE));

        srvAllowed.cluster().active(true);

        for (TestRunnable r : runnables(srvAllowed, clntAllowed))
            allowedRun(r);

        for (TestRunnable r : runnables(srvForbidden, clntForbidden))
            assertForbidden(r);

        for (Supplier<FutureAdapter> s : suppliers(srvAllowed, clntAllowed))
            allowedCancel(s);

        for (Supplier<FutureAdapter> s : suppliers(srvForbiddenCancel, clntForbiddenCancel))
            forbiddenCancel(s);
    }

    /**
     * @param nodes Array of nodes.
     */
    private Collection<TestRunnable> runnables(Ignite... nodes) {
        Function<Ignite, TestRunnable[]> f = (node) -> new TestRunnable[] {
            () -> node.compute().execute(TEST_COMPUTE_TASK, 0),
            () -> node.compute().executeAsync(TEST_COMPUTE_TASK, 0).get(),
            () -> node.compute().broadcast(TEST_CALLABLE),
            () -> node.compute().broadcastAsync(TEST_CALLABLE).get(),
            () -> node.compute().call(TEST_CALLABLE),
            () -> node.compute().callAsync(TEST_CALLABLE).get(),
            () -> node.compute().run(TEST_RUNNABLE),
            () -> node.compute().runAsync(TEST_RUNNABLE).get(),
            () -> node.compute().apply(TEST_CLOSURE, new Object()),
            () -> node.compute().applyAsync(TEST_CLOSURE, new Object()).get(),
            () -> node.executorService().submit(TEST_CALLABLE).get(),
            () -> node.executorService().invokeAll(singletonList(TEST_CALLABLE)),
            () -> node.executorService().invokeAny(singletonList(TEST_CALLABLE))
        };

        List<TestRunnable> res = new ArrayList<>();

        for (Ignite node : nodes)
            res.addAll(Arrays.asList(f.apply(node)));

        return res;
    }

    /**
     *
     */
    private List<Supplier<FutureAdapter>> suppliers(Ignite... nodes) {
        List<Supplier<FutureAdapter>> res = new ArrayList<>();

        for (Ignite node : nodes) {
            res.add(() -> new FutureAdapter(node.compute().broadcastAsync(TEST_CALLABLE)));
            res.add(() -> new FutureAdapter(node.compute().callAsync(TEST_CALLABLE)));
            res.add(() -> new FutureAdapter(node.compute().runAsync(TEST_RUNNABLE)));
            res.add(() -> new FutureAdapter(node.compute().applyAsync(TEST_CLOSURE, new Object())));
            res.add(() -> new FutureAdapter(node.compute().executeAsync(TEST_COMPUTE_TASK, 0)));
            res.add(() -> new FutureAdapter(node.executorService().submit(TEST_CALLABLE)));
        }

        return res;
    }

    /**
     * @param perms Permissions.
     */
    private SecurityPermissionSet permissions(SecurityPermission... perms) {
        return builder()
            .appendTaskPermissions(TEST_COMPUTE_TASK.getClass().getName(), perms)
            .appendTaskPermissions(TEST_CALLABLE.getClass().getName(), perms)
            .appendTaskPermissions(TEST_RUNNABLE.getClass().getName(), perms)
            .appendTaskPermissions(TEST_CLOSURE.getClass().getName(), perms)
            .build();
    }

    /**
     * @param r TestRunnable.
     */
    private void allowedRun(TestRunnable r) {
        IS_EXECUTED.set(false);

        try {
            r.run();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertTrue(IS_EXECUTED.get());
    }

    /**
     * @param s Supplier.
     */
    private void forbiddenCancel(Supplier<FutureAdapter> s) {
        RNT_LOCK.lock();

        try {
            FutureAdapter f = s.get();

            assertForbidden(f::cancel);
        }
        finally {
            RNT_LOCK.unlock();
        }
    }

    /**
     * @param s Supplier.
     */
    private void allowedCancel(Supplier<FutureAdapter> s) {
        RNT_LOCK.lock();

        try {
            FutureAdapter f = s.get();

            f.cancel();

            assertThat(f.isCancelled(), is(true));
        }
        finally {
            RNT_LOCK.unlock();
        }
    }

    /**
     *
     */
    private static class FutureAdapter {
        /** Ignite future. */
        private final IgniteFuture igniteFut;

        /** Future. */
        private final Future fut;

        /**
         * @param igniteFut Ignite future.
         */
        public FutureAdapter(IgniteFuture igniteFut) {
            assert igniteFut != null;

            this.igniteFut = igniteFut;
            fut = null;
        }

        /**
         * @param fut Future.
         */
        public FutureAdapter(Future fut) {
            assert fut != null;

            this.fut = fut;
            igniteFut = null;
        }

        /**
         *
         */
        public void cancel() {
            if (igniteFut != null)
                igniteFut.cancel();
            else
                fut.cancel(true);
        }

        /**
         *
         */
        public Object get() throws ExecutionException, InterruptedException {
            return igniteFut != null ? igniteFut.get() : fut.get();
        }

        /**
         *
         */
        public boolean isCancelled() {
            return igniteFut != null ? igniteFut.isCancelled() : fut.isCancelled();
        }
    }

    /**
     * Abstract test compute task.
     */
    private static class TestComputeTask implements ComputeTask<Object, Object> {
        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) {
            IS_EXECUTED.set(true);

            return Collections.singletonMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        // no-op
                    }

                    @Override public Object execute() {
                        syncForCancel();

                        return null;
                    }
                }, subgrid.stream().findFirst().get()
            );
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
