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

package org.apache.ignite.internal.processor.security.compute;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;

/**
 * Test task execute permission for compute task on Client node.
 */
public class ClientNodeTaskExecutePermissionForComputeTaskTest extends AbstractTaskExecutePermissionTest {
    /** Allowed task. */
    private static final AllowedTask ALLOWED_TASK = new AllowedTask();

    /** Forbidden task. */
    private static final ForbiddenTask FORBIDDEN_TASK = new ForbiddenTask();

    /**
     * @throws Exception If failed.
     */
    public void testExecute() throws Exception {
        Ignite node = startGrid("client_execute",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_TASK.getClass().getName(), TASK_EXECUTE, TASK_CANCEL)
                .appendTaskPermissions(FORBIDDEN_TASK.getClass().getName(), EMPTY_PERMS)
                .build(), isClient()
        );

        allowRun(() -> node.compute().execute(ALLOWED_TASK, 0));
        forbiddenRun(() -> node.compute().execute(FORBIDDEN_TASK, 0));

        allowRun(() -> node.compute().executeAsync(ALLOWED_TASK, 0).get());
        forbiddenRun(() -> node.compute().executeAsync(FORBIDDEN_TASK, 0).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllowedCancel() throws Exception {
        Ignite node = startGrid("client_allowed_cancel",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_TASK.getClass().getName(), TASK_EXECUTE, TASK_CANCEL)
                .build(), isClient()
        );

        ComputeTaskFuture f = node.compute().executeAsync(ALLOWED_TASK, 0);

        f.cancel();

        forbiddenRun(f::get, IgniteFutureCancelledException.class);
    }

    /**
     * @throws Exception If failed.
     */
    public void testForbiddenCancel() throws Exception {
        Ignite node = startGrid("client_forbidden_cancel",
            builder().defaultAllowAll(true)
                .appendTaskPermissions(ALLOWED_TASK.getClass().getName(), TASK_EXECUTE)
                .build(), isClient()
        );

        ComputeTaskFuture f = node.compute().executeAsync(ALLOWED_TASK, 0);

        forbiddenRun(f::cancel);
    }

    /**
     * Allowed task class.
     */
    static class AllowedTask extends TestComputeTask {
        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            JINGLE_BELL.set(true);

            return super.map(subgrid, arg);
        }
    }

    /**
     * Forbidden task class.
     */
    static class ForbiddenTask extends TestComputeTask {
        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            fail("Should not be invoked.");

            return super.map(subgrid, arg);
        }
    }

    /**
     * Abstract test compute task.
     */
    abstract static class TestComputeTask implements ComputeTask<Object, Object> {
        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            return Collections.singletonMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        // no-op
                    }

                    @Override public Object execute() throws IgniteException {
                        return null;
                    }
                }, subgrid.stream().findFirst().get()
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
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
