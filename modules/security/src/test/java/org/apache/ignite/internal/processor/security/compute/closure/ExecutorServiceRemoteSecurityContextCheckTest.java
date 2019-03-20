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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing operation security context when the service task is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to feature call nodes that starts service task. That service task is executed
 * on feature transition nodes and broadcasts a task to endpoint nodes. On every step, it is performed verification that
 * operation security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class ExecutorServiceRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
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
            .expect(SRV_FEATURE_CALL, 1)
            .expect(CLNT_FEATURE_CALL, 1)
            .expect(SRV_FEATURE_TRANSITION, 2)
            .expect(CLNT_FEATURE_TRANSITION, 2)
            .expect(SRV_ENDPOINT, 4)
            .expect(CLNT_ENDPOINT, 4);
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
     * Performs test case.
     */
    private void runAndCheck(IgniteEx initiator) {
        runAndCheck(secSubjectId(initiator),
            () -> compute(initiator, featureCalls()).broadcast(
                () -> {
                    register();

                    Ignite loc = Ignition.localIgnite();

                    for (UUID nodeId : featureTransitions()) {
                        ExecutorService svc = loc.executorService(loc.cluster().forNodeId(nodeId));

                        try {
                            svc.submit(new TestIgniteRunnable(endpoints())).get();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            )
        );
    }

    /**
     * Runnable for tests.
     */
    static class TestIgniteRunnable implements IgniteRunnable {
        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        public TestIgniteRunnable(Collection<UUID> endpoints) {
            assert !endpoints.isEmpty();

            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            register();

            compute(Ignition.localIgnite(), endpoints)
                .broadcast(() -> register());
        }
    }
}
