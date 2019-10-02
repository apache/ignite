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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
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
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

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
        waitForCancel();

        IS_EXECUTED.set(true);

        return null;
    };

    /** Test runnable. */
    private static final IgniteRunnable TEST_RUNNABLE = () -> {
        waitForCancel();

        IS_EXECUTED.set(true);
    };

    /** Test closure. */
    private static final IgniteClosure<Object, Object> TEST_CLOSURE = a -> {
        waitForCancel();

        IS_EXECUTED.set(true);

        return null;
    };

    /** Waits for InterruptedException on RNT_LOCK. */
    private static void waitForCancel() {
        boolean isLocked = false;

        try {
            isLocked = RNT_LOCK.tryLock(RNT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

            if (!isLocked)
                throw new IgniteException("tryLock should succeed or interrupted");
        }
        catch (InterruptedException e) {
            // This is expected.
        }
        finally {
            if (isLocked)
                RNT_LOCK.unlock();
        }
    }

    /** */
    @Test
    public void test() throws Exception {
        Ignite srvAllowed = startGrid("srv_allowed", permissions(TASK_EXECUTE, TASK_CANCEL), false);

        Ignite srvForbidden = startGrid("srv_forbidden", permissions(EMPTY_PERMS), false);

        Ignite srvForbiddenCancel = startGrid("srv_forbidden_cnl", permissions(TASK_EXECUTE), false);

        Ignite clntAllowed = startGrid("clnt_allowed", permissions(TASK_EXECUTE, TASK_CANCEL), true);

        Ignite clntForbidden = startGrid("clnt_forbidden", permissions(EMPTY_PERMS), true);

        Ignite clntForbiddenCancel = startGrid("clnt_forbidden_cnl", permissions(TASK_EXECUTE), true);

        srvAllowed.cluster().active(true);

        operations(srvAllowed, clntAllowed).forEach(this::runOperation);

        operations(srvForbidden, clntForbidden).forEach(op -> assertThrowsWithCause(op, SecurityException.class));

        asyncOperations(srvAllowed, clntAllowed).forEach(this::runOperationCancel);

        asyncOperations(srvForbiddenCancel, clntForbiddenCancel).forEach(op ->
            assertThrowsWithCause(() -> runOperationCancel(op), SecurityException.class));
    }

    /**
     * @param nodes Array of nodes.
     */
    private Stream<RunnableX> operations(Ignite... nodes) {
        Function<Ignite, Stream<RunnableX>> nodeOps = (node) -> Stream.of(
            () -> node.compute().execute(TEST_COMPUTE_TASK, 0),
            () -> node.compute().broadcast(TEST_CALLABLE),
            () -> node.compute().call(TEST_CALLABLE),
            () -> node.compute().run(TEST_RUNNABLE),
            () -> node.compute().apply(TEST_CLOSURE, new Object()),
            () -> node.executorService().invokeAll(singletonList(TEST_CALLABLE)),
            () -> node.executorService().invokeAny(singletonList(TEST_CALLABLE))
        );

        Stream<RunnableX> ops = Arrays.stream(nodes).map(nodeOps).flatMap(identity());

        return Stream.concat(ops, asyncOperations(nodes).map(s -> () -> s.get().get()));
    }

    /** */
    private Stream<Supplier<Future>> asyncOperations(Ignite... nodes) {
        Function<Ignite, Stream<Supplier<Future>>> nodeOps = (node) -> Stream.of(
            () -> new TestFutureAdapter<>(node.compute().executeAsync(TEST_COMPUTE_TASK, 0)),
            () -> new TestFutureAdapter<>(node.compute().broadcastAsync(TEST_CALLABLE)),
            () -> new TestFutureAdapter<>(node.compute().callAsync(TEST_CALLABLE)),
            () -> new TestFutureAdapter<>(node.compute().runAsync(TEST_RUNNABLE)),
            () -> new TestFutureAdapter<>(node.compute().applyAsync(TEST_CLOSURE, new Object())),
            () -> node.executorService().submit(TEST_CALLABLE)
        );

        return Arrays.stream(nodes).map(nodeOps).flatMap(identity());
    }

    /**
     * @param perms Permissions.
     */
    private SecurityPermissionSet permissions(SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .appendTaskPermissions(TEST_COMPUTE_TASK.getClass().getName(), perms)
            .appendTaskPermissions(TEST_CALLABLE.getClass().getName(), perms)
            .appendTaskPermissions(TEST_RUNNABLE.getClass().getName(), perms)
            .appendTaskPermissions(TEST_CLOSURE.getClass().getName(), perms)
            .build();
    }

    /**
     * @param r TestRunnable.
     */
    private void runOperation(Runnable r) {
        IS_EXECUTED.set(false);

        r.run();

        assertTrue(IS_EXECUTED.get());
    }

    /**
     * @param s Supplier.
     */
    private void runOperationCancel(Supplier<Future> s) {
        RNT_LOCK.lock();

        try {
            Future f = s.get();

            f.cancel(true);

            assertTrue(f.isCancelled());
        }
        finally {
            RNT_LOCK.unlock();
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
                        // No-op.
                    }

                    @Override public Object execute() {
                        waitForCancel();

                        return null;
                    }
                }, subgrid.stream().findFirst().orElseThrow(IllegalStateException::new)
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
