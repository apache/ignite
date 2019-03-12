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

package org.apache.ignite.internal.processor.security.messaging;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the message listener is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class MessagingRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Barrier. */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(3);

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
    public static final String CLNT_ENDPOINT = "clnt_endpoint";

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

        startGrid(CLNT_ENDPOINT, allowAllPermissionSet());

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
        fail("https://issues.apache.org/jira/browse/IGNITE-11410");

        runAndCheck(grid(SRV_INITIATOR), grid(CLNT_INITIATOR));
        runAndCheck(grid(CLNT_INITIATOR), grid(SRV_INITIATOR));
    }

    /**
     * Performs the test.
     *
     * @param initiator Initiator node.
     * @param evt Node that generates an event.
     */
    private void runAndCheck(IgniteEx initiator, IgniteEx evt) {
        runAndCheck(
            secSubjectId(initiator),
            () -> {
                for (UUID nodeId : featureCalls()) {
                    compute(initiator, nodeId).broadcast(new IgniteRunnable() {
                        @Override public void run() {
                            register();

                            BARRIER.reset();

                            Ignite loc = Ignition.localIgnite();

                            IgniteMessaging messaging = loc.message(loc.cluster().forNodeIds(featureTransitions()));

                            String topic = "HOT_TOPIC";

                            UUID lsnrId = messaging.remoteListen(topic, new TestListener(endpoints()));

                            try {
                                evt.message().send(topic, "Fire!");
                            }
                            finally {
                                barrierAwait();

                                messaging.stopRemoteListen(lsnrId);
                            }
                        }
                    });
                }
            }
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
     * Call await method on {@link #BARRIER} with {@link GridAbstractTest#getTestTimeout()} timeout.
     */
    private static void barrierAwait() {
        try {
            BARRIER.await(getTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            fail(e.toString());
        }
    }

    /**
     * @return Barrier timeout.
     */
    private static long getTimeout() {
        String timeout = GridTestProperties.getProperty("test.timeout");

        if (timeout != null)
            return Long.parseLong(timeout);

        return GridTestUtils.DFLT_TEST_TIMEOUT;
    }

    /**
     *
     */
    static class TestListener implements IgniteBiPredicate<UUID, String> {
        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        public TestListener(Collection<UUID> endpoints) {
            assert !endpoints.isEmpty();

            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(UUID o, String o2) {
            try {
                register();

                compute(Ignition.localIgnite(), endpoints).broadcast(new IgniteRunnable() {
                    @Override public void run() {
                        register();
                    }
                });

                return true;
            }
            finally {
                barrierAwait();
            }
        }
    }
}
