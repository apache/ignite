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

package org.apache.ignite.internal.processors.security.compute.closure;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.Ignition.localIgnite;

/**
 * Testing operation security context when the compute task is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to 'run' nodes that starts compute task. That compute task is executed on
 * 'check' nodes and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification that operation
 * security context is the initiator context.
 */
public class ComputeTaskRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        startClientAllowAll(CLNT_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        startClientAllowAll(CLNT_ENDPOINT);

        G.allGrids().get(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void test() {
        runAndCheck(operations());
    }

    /**
     * @return Stream of check cases.
     */
    private Stream<IgniteRunnable> operations() {
        return Stream.of(
            () -> localIgnite().compute().execute(new ComputeTaskClosure(nodesToCheckIds(), endpointIds()), 0),
            () -> localIgnite().compute().executeAsync(new ComputeTaskClosure(nodesToCheckIds(), endpointIds()), 0).get()
        );
    }

    /**
     * Compute task for tests.
     */
    static class ComputeTaskClosure implements ComputeTask<Integer, Integer> {
        /** Collection of transition node ids. */
        private final Collection<UUID> remotes;

        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /** Local ignite. */
        @IgniteInstanceResource
        protected transient Ignite loc;

        /**
         * @param remotes Collection of transition node ids.
         * @param endpoints Collection of endpoint node ids.
         */
        public ComputeTaskClosure(Collection<UUID> remotes, Collection<UUID> endpoints) {
            this.remotes = remotes;
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Integer arg) {
            Map<ComputeJob, ClusterNode> res = new HashMap<>();

            for (UUID id : remotes) {
                res.put(
                    new ComputeJob() {
                        @IgniteInstanceResource
                        private Ignite loc;

                        @Override public void cancel() {
                            // no-op
                        }

                        @Override public Object execute() {
                            VERIFIER.register(OPERATION_CHECK);

                            compute(loc, endpoints).broadcast(() -> VERIFIER.register(OPERATION_ENDPOINT));

                            return null;
                        }
                    }, loc.cluster().node(id)
                );
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
