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
import java.util.function.Consumer;
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

/**
 * Testing permissions when the compute closure is executed cache operations on remote node.
 */
public class DistributedClosureRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
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
    @Override protected void setupVerifier(AbstractRemoteSecurityContextCheckTest.Verifier verifier) {
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
            () -> compute(initiator, transitions())
                .broadcast((IgniteRunnable)new CommonClosure(endpoints(), true) {
                    @Override protected void transit(IgniteCompute cmp) {
                        cmp.broadcast((IgniteRunnable)endpointClosure());
                    }
                }));

        runAndCheck(secSubjectId(initiator),
            () -> compute(initiator, transitions())
                .broadcastAsync((IgniteRunnable)new CommonClosure(endpoints(), true) {
                    @Override protected void transit(IgniteCompute cmp) {
                        cmp.broadcastAsync((IgniteRunnable)endpointClosure()).get();
                    }
                }).get());

        runAndCheck(initiator,
            (cmp) -> cmp.call(new CommonClosure(endpoints()) {
                @Override protected void transit(IgniteCompute cmp) {
                    cmp.call(endpointClosure());
                }
            }));

        runAndCheck(initiator,
            (cmp) -> cmp.callAsync(new CommonClosure(endpoints()) {
                @Override protected void transit(IgniteCompute cmp) {
                    cmp.callAsync(endpointClosure()).get();
                }
            }).get());

        runAndCheck(initiator,
            (cmp) -> cmp.run(new CommonClosure(endpoints()) {
                @Override protected void transit(IgniteCompute cmp) {
                    cmp.run(endpointClosure());
                }
            }));

        runAndCheck(initiator,
            (cmp) -> cmp.runAsync(new CommonClosure(endpoints()) {
                @Override protected void transit(IgniteCompute cmp) {
                    cmp.runAsync(endpointClosure()).get();
                }
            }).get());

        runAndCheck(initiator,
            (cmp) -> cmp.apply(new CommonClosure(endpoints()) {
                @Override protected void transit(IgniteCompute cmp) {
                    cmp.apply(endpointClosure(), new Object());
                }
            }, new Object()));

        runAndCheck(initiator,
            (cmp) -> cmp.applyAsync(new CommonClosure(endpoints()) {
                @Override protected void transit(IgniteCompute cmp) {
                    cmp.applyAsync(endpointClosure(), new Object()).get();
                }
            }, new Object()).get());
    }

    /**
     * Performs test case.
     */
    private void runAndCheck(IgniteEx initiator, Consumer<IgniteCompute> c) {
        runAndCheck(
            secSubjectId(initiator),
            () -> {
                for (UUID nodeId : transitions())
                    c.accept(compute(initiator, nodeId));
            }
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
     * @return IgniteCompute is produced by passed node for cluster group that contains nodes with ids from collection.
     */
    private static IgniteCompute compute(Ignite ignite, Collection<UUID> ids) {
        return ignite.compute(ignite.cluster().forNodeIds(ids));
    }

    /**
     * @return IgniteCompute is produced by passed node for cluster group that contains node with id.
     */
    private static IgniteCompute compute(Ignite ignite, UUID id) {
        return ignite.compute(ignite.cluster().forNodeId(id));
    }

    /**
     * Common closure for tests.
     */
    static class CommonClosure implements IgniteRunnable, IgniteCallable<Object>,
        IgniteClosure<Object, Object> {
        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /** If true then execution is broadcast. */
        private final boolean isBroadcast;

        /**
         * @param endpoints Collection of endpoint node ids.
         * @param isBroadcast If true then execution is broadcast.
         */
        public CommonClosure(Collection<UUID> endpoints, boolean isBroadcast) {
            this.endpoints = endpoints;
            this.isBroadcast = isBroadcast;
        }

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        public CommonClosure(Collection<UUID> endpoints) {
            this(endpoints, false);
        }

        /**
         * Main logic of CommonClosure.
         */
        private void body() {
            verify();

            if (!endpoints.isEmpty()) {
                Ignite ignite = Ignition.localIgnite();

                if (isBroadcast)
                    transit(compute(ignite, endpoints));
                else {
                    for (UUID id : endpoints)
                        transit(compute(ignite, id));
                }
            }
        }

        /**
         * @return CommonClosure to execute on an endpoint node.
         */
        protected CommonClosure endpointClosure() {
            return new CommonClosure(Collections.emptyList());
        }

        /**
         * Executes transition invoke.
         *
         * @param cmp IgniteCompute.
         */
        protected void transit(IgniteCompute cmp) {
            //no-op by default
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
