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
 * Testing permissions when the service task is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class ExecutorServiceRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
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
     * Performs test case.
     */
    private void runAndCheck(IgniteEx initiator) {
        runAndCheck(secSubjectId(initiator),
            () -> {
                for (UUID nodeId : transitions()) {
                    ExecutorService svc = initiator.executorService(initiator.cluster().forNodeId(nodeId));

                    try {
                        svc.submit(new TestIgniteRunnable(endpoints())).get();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
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
     * Runnable for tests.
     */
    static class TestIgniteRunnable implements IgniteRunnable {
        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        public TestIgniteRunnable(Collection<UUID> endpoints) {
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            verify();

            if (!endpoints.isEmpty()) {
                Ignite ignite = Ignition.localIgnite();

                try {
                    for (UUID nodeId : endpoints) {
                        ignite.executorService(ignite.cluster().forNodeId(nodeId))
                            .submit(new TestIgniteRunnable(Collections.emptyList()))
                            .get();
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
