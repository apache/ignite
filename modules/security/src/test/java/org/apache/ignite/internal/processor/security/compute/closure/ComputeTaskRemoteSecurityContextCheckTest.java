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

package org.apache.ignite.internal.processor.security.compute.closure;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Testing permissions when the compute task is executed cache operations on remote node.
 */
public class ComputeTaskRemoteSecurityContextCheckTest extends AbstractComputeRemoteSecurityContextCheckTest {
    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR));
        runAndCheck(grid(CLNT_INITIATOR));
    }

    /**
     * @param initiator Node that initiates an execution.
     */
    private void runAndCheck(IgniteEx initiator) {
        runAndCheck(initiator,
            () -> initiator.compute().execute(
                new TestComputeTask(transitions(), endpoints(), false), 0
            )
        );

        runAndCheck(initiator,
            () -> initiator.compute().executeAsync(
                new TestComputeTask(transitions(), endpoints(), true), 0
            ).get()
        );
    }

    /**
     * Compute task for tests.
     */
    static class TestComputeTask implements ComputeTask<Integer, Integer> {
        /** Collection of transition node ids. */
        private final Collection<UUID> remotes;

        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /** If true then run async. */
        private final boolean isAsync;

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remotes Collection of transition node ids.
         * @param endpoints Collection of endpoint node ids.
         * @param isAsync If true then run async.
         */
        public TestComputeTask(Collection<UUID> remotes, Collection<UUID> endpoints, boolean isAsync) {
            this.remotes = remotes;
            this.endpoints = endpoints;
            this.isAsync = isAsync;
        }

        /**
         * @param remotes Collection of transition node ids.
         */
        public TestComputeTask(Collection<UUID> remotes) {
            this(remotes, Collections.emptyList(), false);
        }

        /** {@inheritDoc} */
        @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Integer arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> res = new HashMap<>();

            for (UUID id : remotes) {
                res.put(
                    new ComputeJob() {
                        @IgniteInstanceResource
                        private Ignite loc;

                        @Override public void cancel() {
                            // no-op
                        }

                        @Override public Object execute() throws IgniteException {
                            VERIFIER.verify(loc);

                            if (!endpoints.isEmpty()) {
                                if (isAsync)
                                    loc.compute().executeAsync(new TestComputeTask(endpoints), 0).get();
                                else
                                    loc.compute().execute(new TestComputeTask(endpoints), 0);
                            }

                            return null;
                        }
                    }, loc.cluster().node(id)
                );
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> rcvd) throws IgniteException {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}