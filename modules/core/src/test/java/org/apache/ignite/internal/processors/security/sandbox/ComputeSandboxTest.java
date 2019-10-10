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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Checks that user-defined code for compute operations is executed inside the sandbox.
 */
public class ComputeSandboxTest extends AbstractSandboxTest {
    /** */
    private static final TestComputeTask COMPUTE_TASK = new TestComputeTask(START_THREAD_RUNNABLE);

    /** */
    private static final IgniteCallable<Object> CALLABLE = () -> {
        START_THREAD_RUNNABLE.run();

        return null;
    };

    /** */
    private static final IgniteClosure<Object, Object> CLOSURE = a -> {
        START_THREAD_RUNNABLE.run();

        return null;
    };

    /** */
    @Test
    public void test() throws Exception {
        prepareCluster();

        Ignite clntAllowed = grid(CLNT_ALLOWED_THREAD_START);

        Ignite clntFrobidden = grid(CLNT_FORBIDDEN_THREAD_START);

        computeOperations(clntAllowed).forEach(this::runOperation);
        computeOperations(clntFrobidden).forEach(op -> runForbiddenOperation(op, AccessControlException.class));

        executorServiceOperations(clntAllowed).forEach(this::runOperation);
        executorServiceOperations(clntFrobidden).forEach(op -> runForbiddenOperation(op, IgniteException.class));
    }

    /**
     * @return Stream of Compute operations to test.
     */
    private Stream<GridTestUtils.RunnableX> computeOperations(Ignite node) {
        return Stream.of(
            () -> node.compute().execute(COMPUTE_TASK, 0),
            () -> node.compute().broadcast(CALLABLE),
            () -> node.compute().call(CALLABLE),
            () -> node.compute().run(START_THREAD_RUNNABLE),
            () -> node.compute().apply(CLOSURE, new Object()),

            () -> new TestFutureAdapter<>(node.compute().executeAsync(COMPUTE_TASK, 0)).get(),
            () -> new TestFutureAdapter<>(node.compute().broadcastAsync(CALLABLE)).get(),
            () -> new TestFutureAdapter<>(node.compute().callAsync(CALLABLE)).get(),
            () -> new TestFutureAdapter<>(node.compute().runAsync(START_THREAD_RUNNABLE)).get(),
            () -> new TestFutureAdapter<>(node.compute().applyAsync(CLOSURE, new Object())).get()
        );
    }

    /**
     * @return Stream of ExecutorService operations to test.
     */
    private Stream<GridTestUtils.RunnableX> executorServiceOperations(Ignite node) {
        return Stream.of(
            () -> node.executorService().invokeAll(singletonList(CALLABLE))
                .stream().findFirst().orElseThrow(IgniteException::new).get(),
            () -> node.executorService().invokeAny(singletonList(CALLABLE)),
            () -> node.executorService().submit(CALLABLE).get()
        );
    }

    /** */
    static class TestComputeTask implements ComputeTask<Object, Object> {
        /** */
        private final IgniteRunnable r;

        /** */
        public TestComputeTask(IgniteRunnable r) {
            this.r = r;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            Object arg) throws IgniteException {
            return Collections.singletonMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        // No-op.
                    }

                    @Override public Object execute() {
                        r.run();

                        return null;
                    }
                }, subgrid.stream().findFirst().orElseThrow(IllegalStateException::new)
            );
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> rcvd) throws IgniteException {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
