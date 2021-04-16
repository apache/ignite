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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Checks that a remote listener for IgniteMessaging is executed inside the sandbox.
 */
public class MessagingSandboxTest extends AbstractSandboxTest {
    /** Node to send messages. */
    private static final String SRV_SENDER = "srv_sender";

    /** Latch. */
    private static volatile CountDownLatch latch;

    /** Error. */
    private static volatile AccessControlException error;


    /** {@inheritDoc} */
    @Override protected void prepareCluster() throws Exception {
        startGrid(SRV_SENDER, ALLOW_ALL, false);

        super.prepareCluster();
    }

    /** */
    @Test
    public void testRemoteListen() {
        testMessaging((m, t) -> m.remoteListen(t, listener()));
    }

    /** */
    @Test
    public void testRemoteListenAsync() {
        testMessaging((m, t) -> m.remoteListenAsync(t, listener()).get());
    }

    /** */
    private void testMessaging(BiFunction<IgniteMessaging, String, UUID> func) {
        execute(grid(CLNT_ALLOWED_WRITE_PROP), func, false);
        execute(grid(CLNT_FORBIDDEN_WRITE_PROP), func, true);
    }

    /** */
    private void execute(Ignite node, BiFunction<IgniteMessaging, String, UUID> func, boolean isForbiddenCase) {
        final String topic = "test_topic";

        IgniteMessaging messaging = node.message(node.cluster().forNodeId(grid(SRV).localNode().id()));

        UUID listenerId = func.apply(messaging, topic);

        try {
            GridTestUtils.RunnableX r = () -> {
                error = null;

                latch = new CountDownLatch(1);

                grid(SRV_SENDER).message().send(topic, "Hello!");

                latch.await(10, TimeUnit.SECONDS);

                if (error != null)
                    throw error;
            };

            if (isForbiddenCase)
                runForbiddenOperation(r, AccessControlException.class);
            else
                runOperation(r);
        }
        finally {
            messaging.stopRemoteListen(listenerId);
        }
    }

    /** */
    private IgniteBiPredicate<UUID, ?> listener() {
        return (uuid, o) -> {
            try {
                controlAction();
            }
            catch (AccessControlException e) {
                error = e;
            }
            finally {
                latch.countDown();
            }

            return false;
        };
    }
}
