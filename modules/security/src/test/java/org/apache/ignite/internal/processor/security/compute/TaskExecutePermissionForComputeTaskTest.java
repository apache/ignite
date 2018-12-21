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
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;

/**
 * Test task execute permission for compute task.
 */
public class TaskExecutePermissionForComputeTaskTest extends AbstractTaskExecutePermissionTest {
    /** Allowed task. */
    private static final AllowedTask TEST_TASK = new AllowedTask();

    /** {@inheritDoc} */
    @Override protected SecurityPermissionSet permissions(SecurityPermission... perms) {
        return builder().defaultAllowAll(true)
            .appendTaskPermissions(TEST_TASK.getClass().getName(), perms)
            .build();
    }

    /** {@inheritDoc} */
    @Override protected TestRunnable[] runnables(Ignite node){
        return new TestRunnable[]{
            () -> node.compute().execute(TEST_TASK, 0),
            () -> node.compute().executeAsync(TEST_TASK, 0).get()
        };
    }

    /** {@inheritDoc} */
    @Override protected Supplier<FutureAdapter> cancelSupplier(Ignite node) {
        return () -> new FutureAdapter(node.compute().executeAsync(TEST_TASK, 0));
    }

    /**
     * Abstract test compute task.
     */
    static class AllowedTask implements ComputeTask<Object, Object> {
        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            IS_EXECUTED.set(true);

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
