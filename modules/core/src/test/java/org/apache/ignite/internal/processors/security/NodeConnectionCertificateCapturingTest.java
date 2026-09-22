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

package org.apache.ignite.internal.processors.security;

import java.security.Permissions;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CERTIFICATES;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/** */
public class NodeConnectionCertificateCapturingTest extends AbstractSecurityTest {
    /** */
    private static final Collection<AuthenticationEvent> NODE_AUTHENTICATION_EVENTS = new ConcurrentLinkedQueue<>();

    /** */
    @Test
    public void testNodeConnectionCertificateCapturing() throws Exception {
        checkNewNodeAuthenticationByClusterNodes(0, false, 1);
        checkNewNodeAuthenticationByClusterNodes(1, false, 2);
        checkNewNodeAuthenticationByClusterNodes(2, false, 3);
        checkNewNodeAuthenticationByClusterNodes(3, true, 3);

        // Checks nodes restart.
        stopGrid(2);
        stopGrid(3);

        checkNewNodeAuthenticationByClusterNodes(3, true, 2);
        checkNewNodeAuthenticationByClusterNodes(2, false, 3);

        // Checks client node reconnect.
        NODE_AUTHENTICATION_EVENTS.clear();

        CountDownLatch cliNodeReconnectedLatch = new CountDownLatch(1);

        grid(3).events().localListen(evt -> {
            cliNodeReconnectedLatch.countDown();

            return true;
        }, EVT_CLIENT_NODE_RECONNECTED);

        grid(0).context().discovery().failNode(grid(3).localNode().id(), "test");

        assertTrue(cliNodeReconnectedLatch.await(getTestTimeout(), MILLISECONDS));

        checkNodeAuthenticationByClusterNodes(3, grid(3).localNode().id(), true, 3);
    }

    /** */
    private void checkNewNodeAuthenticationByClusterNodes(int authNodeIdx, boolean isClient, int expAuthCnt) throws Exception {
        NODE_AUTHENTICATION_EVENTS.clear();

        UUID authNodeId = startGrid(authNodeIdx, isClient).cluster().localNode().id();

        checkNodeAuthenticationByClusterNodes(authNodeIdx, authNodeId, isClient, expAuthCnt);
    }

    /** */
    private void checkNodeAuthenticationByClusterNodes(int authNodeIdx, UUID authNodeId, boolean isClient, int expAuthCnt) {
        assertEquals(expAuthCnt, NODE_AUTHENTICATION_EVENTS.size());

        for (AuthenticationEvent auth : NODE_AUTHENTICATION_EVENTS) {
            if (auth.clusterNodeId.equals(authNodeId))
                assertNull(auth.certs);
            else {
                assertEquals(2, auth.certs.length);

                X509Certificate cert = (X509Certificate)auth.certs[0];

                assertEquals(isClient ? "CN=client" : "CN=node0" + (authNodeIdx + 1), cert.getSubjectDN().getName());
            }
        }

        for (Ignite ignite : G.allGrids()) {
            for (ClusterNode node : ignite.cluster().nodes()) {
                assertNull(((TcpDiscoveryNode)node).getAttributes().get(ATTR_SECURITY_CREDENTIALS));
                assertNull(((TcpDiscoveryNode)node).getAttributes().get(ATTR_NODE_CERTIFICATES));
            }
        }
    }

    /** */
    private IgniteEx startGrid(int idx, boolean isClient) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(
            login,
            new SecurityPluginProvider(
                login,
                "",
                isClient ? NO_PERMISSIONS : systemPermissions(JOIN_AS_SERVER),
                null,
                true
            )).setClientMode(isClient);

        cfg.setSslContextFactory(GridTestUtils.sslTrustedFactory(isClient ? "client" : "node0" + (idx + 1), "trustboth"));

        return startGrid(cfg);
    }

    /** */
    private static class SecurityPluginProvider extends TestSecurityPluginProvider {
        /** */
        SecurityPluginProvider(
            String login,
            String pwd,
            SecurityPermissionSet perms,
            Permissions sandboxPerms,
            boolean globalAuth,
            TestSecurityData... clientData
        ) {
            super(login, pwd, perms, sandboxPerms, globalAuth, clientData);
        }

        /** {@inheritDoc} */
        @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
            return new TestSecurityProcessor(
                ctx,
                new TestSecurityData(login, pwd, perms, sandboxPerms),
                Arrays.asList(clientData),
                globalAuth
            ) {
                @Override public SecurityContext authenticateNode(
                    ClusterNode node,
                    SecurityCredentials cred
                ) throws IgniteCheckedException {
                    NODE_AUTHENTICATION_EVENTS.add(new AuthenticationEvent(ctx.localNodeId(), node.attribute(ATTR_NODE_CERTIFICATES)));

                    return super.authenticateNode(node, cred);
                }
            };
        }
    }

    /** */
    private static class AuthenticationEvent {
        /** */
        UUID clusterNodeId;

        /** */
        Certificate[] certs;

        /** */
        AuthenticationEvent(UUID clusterNodeId, Certificate[] certs) {
            this.clusterNodeId = clusterNodeId;
            this.certs = certs;
        }
    }
}
