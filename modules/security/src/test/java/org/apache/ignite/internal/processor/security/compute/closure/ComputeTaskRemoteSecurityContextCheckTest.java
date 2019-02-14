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

import java.util.Arrays;
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
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Testing permissions when the compute task is executed cache operations on remote node.
 */
public class ComputeTaskRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Name of server initiator node. */
    private static final String SRV_INITIATOR = "srv_initiator";

    /** Name of client initiator node. */
    private static final String CLNT_INITIATOR = "clnt_initiator";

    /** Name of server transition node. */
    private static final String SRV_TRANSITION = "srv_transition";

    /** Name of server endpoint node. */
    private static final String SRV_ENDPOINT = "srv_endpoint";

    /** Name of client transition node. */
    private static final String CLNT_TRANSITION = "clnt_transition";

    /** Name of client endpoint node. */
    private static final String CLNT_ENDPOINT = "clnt_endpoint";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startGrid(CLNT_INITIATOR, allowAllPermissionSet(), true);

        startGrid(SRV_TRANSITION, allowAllPermissionSet());

        startGrid(CLNT_TRANSITION, allowAllPermissionSet(), true);

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startGrid(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .add(SRV_TRANSITION, 1)
            .add(CLNT_TRANSITION, 1)
            .add(SRV_ENDPOINT, 2)
            .add(CLNT_ENDPOINT, 2);
    }

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
        runAndCheck(secSubjectId(initiator),
            () -> initiator.compute().execute(
                new TestComputeTask(transitions(), endpoints(), false), 0
            )
        );

        runAndCheck(secSubjectId(initiator),
            () -> initiator.compute().executeAsync(
                new TestComputeTask(transitions(), endpoints(), true), 0
            ).get()
        );
    }

    /**
     * @return Collection of transition node ids.
     */
    private Collection<UUID> transitions() {
        return Arrays.asList(
            grid(SRV_TRANSITION).localNode().id(),
            grid(CLNT_TRANSITION).localNode().id()
        );
    }

    /**
     * @return Collection of endpont nodes ids.
     */
    private Collection<UUID> endpoints() {
        return Arrays.asList(
            grid(SRV_ENDPOINT).localNode().id(),
            grid(CLNT_ENDPOINT).localNode().id()
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
                            verify(loc);

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