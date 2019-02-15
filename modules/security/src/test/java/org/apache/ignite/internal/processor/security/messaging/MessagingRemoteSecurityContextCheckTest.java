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

import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
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

    /** Name of server transition node. */
    private static final String SRV_TRANSITION = "srv_transition";

    /** Name of client transition node. */
    public static final String CLNT_TRANSITION = "clnt_transition";

    /** Name of server endpoint node. */
    private static final String SRV_ENDPOINT = "srv_endpoint";

    /** Name of client endpoint node. */
    public static final String CLNT_ENDPOINT = "clnt_endpoint";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startGrid(CLNT_INITIATOR, allowAllPermissionSet(), true);

        startGrid(SRV_TRANSITION, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startGrid(CLNT_TRANSITION, allowAllPermissionSet(), true);

        startGrid(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .add(SRV_ENDPOINT, 1)
            .add(CLNT_ENDPOINT, 1);
    }

    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR), grid(SRV_TRANSITION));
        runAndCheck(grid(SRV_INITIATOR), grid(CLNT_TRANSITION));

        runAndCheck(grid(CLNT_INITIATOR), grid(SRV_TRANSITION));
        runAndCheck(grid(CLNT_INITIATOR), grid(CLNT_TRANSITION));
    }

    /**
     * Performs the test.
     *
     * @param lsnrNode Node that registers a listener on a remote node.
     * @param evtNode Node that generates an event.
     */
    private void runAndCheck(IgniteEx lsnrNode, IgniteEx evtNode) {
        runAndCheck(secSubjectId(lsnrNode),
            () -> {
                BARRIER.reset();

                IgniteMessaging messaging = lsnrNode.message(
                    lsnrNode.cluster().forNode(grid(SRV_ENDPOINT).localNode(), grid(CLNT_ENDPOINT).localNode())
                );

                String topic = "HOT_TOPIC";

                UUID lsnrId = messaging.remoteListen(topic,
                    new IgniteBiPredicate<UUID, Object>() {
                        @Override public boolean apply(UUID uuid, Object o) {
                            try {
                                verify();

                                return true;
                            }
                            finally {
                                barrierAwait();
                            }
                        }
                    }
                );

                try {
                    evtNode.message().send(topic, "Fire!");
                }
                finally {
                    barrierAwait();

                    messaging.stopRemoteListen(lsnrId);
                }
            }
        );
    }

    /**
     * Call await method on {@link #BARRIER} with {@link GridAbstractTest#getTestTimeout()} timeout.
     */
    private void barrierAwait() {
        try {
            BARRIER.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            fail(e.toString());
        }
    }
}
