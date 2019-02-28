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
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;

import static org.apache.ignite.internal.processor.security.compute.closure.DistributedClosureAdvancedRemoteSecurityContextCheckTest.ComputeInvoke.BROADCAST;

/**
 * Testing permissions when the compute closure is executed cache operations on remote node.
 */
public class DistributedClosureAdvancedRemoteSecurityContextCheckTest
    extends AbstractRemoteSecurityContextCheckTest {
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

        startGrid(CLNT_INITIATOR, allowAllPermissionSet(), true);

        startGrid(SRV_FEATURE_CALL, allowAllPermissionSet());

        startGrid(CLNT_FEATURE_CALL, allowAllPermissionSet(), true);

        startGrid(SRV_FEATURE_TRANSITION, allowAllPermissionSet());

        startGrid(CLNT_FEATURE_TRANSITION, allowAllPermissionSet(), true);

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startGrid(CLNT_ENDPOINT, allowAllPermissionSet(), true);

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
     * @param initiator Initiator node.
     */
    private void runAndCheck(IgniteEx initiator) {
        for (ComputeInvoke ci : ComputeInvoke.values()) {
            runAndCheck(secSubjectId(initiator),
                () -> compute(initiator, featureCalls()).broadcast((IgniteRunnable)
                    new CommonClosure(ci, featureTransitions(), endpoints())
                )
            );
        }
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

    /** Enum for ways to invoke compute. */
    enum ComputeInvoke {
        /** Broadcast. */
        BROADCAST,
        /** Broadcast async. */
        BROADCAST_ASYNC,
        /** Call. */
        CALL,
        /** Call async. */
        CALL_ASYNC,
        /** Run. */
        RUN,
        /** Run async. */
        RUN_ASYNC,
        /** Apply. */
        APPLY,
        /** Apply async. */
        APPLY_ASYNC;

        /**
         * @return True if an invoke is broadcast.
         */
        public boolean isBroadcast() {
            return this == BROADCAST || this == BROADCAST_ASYNC;
        }
    }

    /**
     * Common closure for tests.
     */
    static class CommonClosure implements IgniteRunnable, IgniteCallable<Object>,
        IgniteClosure<Object, Object> {

        /** Type of compute invoke. */
        private final ComputeInvoke cmpInvoke;

        /** Collection of endpoint node ids. */
        private final Collection<UUID> transitions;

        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        public CommonClosure(ComputeInvoke cmpInvoke, Collection<UUID> transitions,
            Collection<UUID> endpoints) {
            assert cmpInvoke != null;
            assert !endpoints.isEmpty();

            this.cmpInvoke = cmpInvoke;
            this.transitions = transitions;
            this.endpoints = endpoints;
        }

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        private CommonClosure(Collection<UUID> endpoints) {
            this(BROADCAST, Collections.emptyList(), endpoints);
        }

        /**
         * Main logic of CommonClosure.
         */
        private void body() {
            verify();

            Ignite ignite = Ignition.localIgnite();

            if (!transitions.isEmpty()) {
                if (cmpInvoke.isBroadcast())
                    transit(compute(ignite, transitions));
                else {
                    for (UUID id : transitions)
                        transit(compute(ignite, id));
                }
            }
            else {
                compute(ignite, endpoints)
                    .broadcast(new IgniteRunnable() {
                        @Override public void run() {
                            verify();
                        }
                    });
            }
        }

        /**
         * Executes transition invoke.
         *
         * @param cmp IgniteCompute.
         */
        private void transit(IgniteCompute cmp) {
            CommonClosure c = new CommonClosure(endpoints);

            switch (cmpInvoke) {
                case BROADCAST: {
                    cmp.broadcast((IgniteRunnable)c);

                    break;
                }
                case BROADCAST_ASYNC: {
                    cmp.broadcastAsync((IgniteRunnable)c).get();

                    break;
                }
                case CALL: {
                    cmp.call(c);

                    break;
                }
                case CALL_ASYNC: {
                    cmp.callAsync(c).get();

                    break;
                }
                case RUN: {
                    cmp.run(c);

                    break;
                }
                case RUN_ASYNC: {
                    cmp.runAsync(c).get();

                    break;
                }
                case APPLY: {
                    cmp.apply(c, new Object());

                    break;
                }
                case APPLY_ASYNC: {
                    cmp.applyAsync(c, new Object()).get();

                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown ComputeInvoke: " + cmpInvoke);
            }
        }

        /** {@inheritDoc} */
        @Override public void run() {
            body();
        }

        /** {@inheritDoc} */
        @Override public Object call() {
            body();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object o) {
            body();

            return null;
        }
    }
}
