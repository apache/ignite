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

package org.apache.ignite.internal.processors.security.discovery;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Tests that discovery custom message processing is performed on the remote nodes in the same security context in
 * which message was sent.
 */
public class DiscoveryCustomMessageSecurityContextTest extends AbstractSecurityTest {
    /** */
    private static final UUID TEST_SECURITY_SUBJECT_ID = UUID.randomUUID();

    /** */
    @Test
    public void testCustomMessageSecurityContext() throws Exception {
        // 12 = 3 nodes in cluster * (1 msg + 1 ack) * 2 (msg sending is performed twice from server and client node).
        CountDownLatch msgSecCtxCheckedLatch = new CountDownLatch(12);

        IgniteEx crd = startGrid(0, false, msgSecCtxCheckedLatch);

        IgniteEx srv = startGrid(1, false, msgSecCtxCheckedLatch);

        IgniteEx cli = startGrid(2, true, msgSecCtxCheckedLatch);

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectId(TEST_SECURITY_SUBJECT_ID);
        authCtx.credentials(new SecurityCredentials("test", ""));

        SecurityContext secCtx = crd.context().security().authenticate(authCtx);

        assertEquals(TEST_SECURITY_SUBJECT_ID, secCtx.subject().id());

        try (OperationSecurityContext ignored = cli.context().security().withContext(TEST_SECURITY_SUBJECT_ID)) {
            cli.context().discovery().sendCustomEvent(new TestCustomMessage());
        }

        try (OperationSecurityContext ignored = srv.context().security().withContext(TEST_SECURITY_SUBJECT_ID)) {
            srv.context().discovery().sendCustomEvent(new TestCustomMessage());
        }

        assertTrue(msgSecCtxCheckedLatch.await(getTestTimeout(), MILLISECONDS));
    }

    /**
     * Starts an Ignite node with custom discovery message listeners that checks the security context in
     * which processing is performed and decrement corresponding latch.
     */
    private IgniteEx startGrid(int idx, boolean isClient, CountDownLatch msgSecCtxCheckedLatch) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        IgniteEx ignite = isClient ? startClientAllowAll(login) : startGridAllowAll(login);

        GridKernalContext ctx = ignite.context();

        ctx.discovery().setCustomEventListener(TestCustomMessage.class,
            (topVer, snd, msg) -> {
                assertEquals(TEST_SECURITY_SUBJECT_ID, ctx.security().securityContext().subject().id());

                msgSecCtxCheckedLatch.countDown();
            });

        ctx.discovery().setCustomEventListener(TestCustomAckMessage.class,
            (topVer, snd, msg) -> {
                assertEquals(TEST_SECURITY_SUBJECT_ID, ctx.security().securityContext().subject().id());

                msgSecCtxCheckedLatch.countDown();
            });

        return ignite;
    }

    /** */
    private static class TestCustomAckMessage implements DiscoveryCustomMessage {
        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return IgniteUuid.randomUuid();
        }

        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(
            GridDiscoveryManager mgr,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache
        ) {
            return null;
        }
    }

    /** */
    private static class TestCustomMessage implements DiscoveryCustomMessage {
        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return IgniteUuid.randomUuid();
        }

        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return new TestCustomAckMessage();
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(
            GridDiscoveryManager mgr,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache
        ) {
            return null;
        }
    }
}
