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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Testing operation security context when the compute task is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to feature call nodes that starts compute task. That compute task is executed
 * on feature transition nodes and broadcasts a task to endpoint nodes. On every step, it is performed verification that
 * operation security context is the initiator context.
 */
public class ComputeTaskRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Name of server initiator node. */
    private static final String SRV_INITIATOR = "srv_initiator";

    /** Name of client initiator node. */
    private static final String CLNT_INITIATOR = "clnt_initiator";

    /** Name of server feature call node. */
    private static final String SRV_FEATURE_CALL = "srv_feature_call";

    /** Name of client feature call node. */
    private static final String CLNT_FEATURE_CALL = "clnt_feature_call";

    /** Name of server feature transit node. */
    private static final String SRV_FEATURE_TRANSITION = "srv_feature_transition";

    /** Name of client feature transit node. */
    private static final String CLNT_FEATURE_TRANSITION = "clnt_feature_transition";

    /** Name of server endpoint node. */
    private static final String SRV_ENDPOINT = "srv_endpoint";

    /** Name of client endpoint node. */
    private static final String CLNT_ENDPOINT = "clnt_endpoint";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_FEATURE_CALL, allowAllPermissionSet());

        startClient(CLNT_FEATURE_CALL, allowAllPermissionSet());

        startGrid(SRV_FEATURE_TRANSITION, allowAllPermissionSet());

        startClient(CLNT_FEATURE_TRANSITION, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .add(SRV_FEATURE_CALL, 1)
            .add(CLNT_FEATURE_CALL, 1)
            .add(SRV_FEATURE_TRANSITION, 2)
            .add(CLNT_FEATURE_TRANSITION, 2)
            .add(SRV_ENDPOINT, 4)
            .add(CLNT_ENDPOINT, 4);
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
            () -> compute(initiator, featureCalls()).broadcast(
                new IgniteRunnable() {
                    @Override public void run() {
                        register();

                        Ignition.localIgnite().compute().execute(
                            new TestComputeTask(featureTransitions(), endpoints()), 0
                        );
                    }
                }
            )
        );

        runAndCheck(secSubjectId(initiator),
            () -> compute(initiator, featureCalls()).broadcast(
                new IgniteRunnable() {
                    @Override public void run() {
                        register();

                        Ignition.localIgnite().compute().executeAsync(
                            new TestComputeTask(featureTransitions(), endpoints()), 0
                        ).get();
                    }
                }
            )
        );
    }

    /**
     * @return Collection of feature call nodes ids.
     */
    private Collection<UUID> featureCalls() {
        return Arrays.asList(
            nodeId(SRV_FEATURE_CALL),
            nodeId(CLNT_FEATURE_CALL)
        );
    }

    /**
     * @return Collection of feature transit nodes ids.
     */
    private Collection<UUID> featureTransitions() {
        return Arrays.asList(
            nodeId(SRV_FEATURE_TRANSITION),
            nodeId(CLNT_FEATURE_TRANSITION)
        );
    }

    /**
     * @return Collection of endpont nodes ids.
     */
    private Collection<UUID> endpoints() {
        return Arrays.asList(
            nodeId(SRV_ENDPOINT),
            nodeId(CLNT_ENDPOINT)
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

        /** Local ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remotes Collection of transition node ids.
         * @param endpoints Collection of endpoint node ids.
         */
        public TestComputeTask(Collection<UUID> remotes, Collection<UUID> endpoints) {
            assert !remotes.isEmpty();
            assert !endpoints.isEmpty();

            this.remotes = remotes;
            this.endpoints = endpoints;
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
                            register();

                            compute(loc, endpoints)
                                .broadcast(() -> register());

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