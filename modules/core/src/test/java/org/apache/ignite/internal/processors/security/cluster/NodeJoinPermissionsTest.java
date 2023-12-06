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

package org.apache.ignite.internal.processors.security.cluster;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.internal.processors.security.impl.TestSecuritySubject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_NODE;

/** */
@RunWith(Parameterized.class)
public class NodeJoinPermissionsTest extends AbstractSecurityTest {
    /** */
    @Parameterized.Parameter
    public boolean isLegacyAuthApproach;

    /** */
    @Parameterized.Parameters(name = "isLegacyAuthorizationApproach={0}")
    public static Object[] parameters() {
        return new Object[] { false, true };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** */
    private IgniteConfiguration configuration(int idx, SecurityPermission... sysPermissions) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        AbstractTestSecurityPluginProvider secPuginProv = isLegacyAuthApproach
            ? new TestSecurityPluginProvider(
                login,
                "",
                systemPermissions(sysPermissions),
                false)
            : new SecurityPluginProvider(
                login,
                "",
                systemPermissions(sysPermissions),
                false);

        return getConfiguration(login, secPuginProv);
    }

    /** */
    @Test
    public void testNodeJoinPermissions() throws Exception {
        // The first node can start successfully without any permissions granted.
        startGrid(configuration(0));

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGrid(configuration(1)),
            IgniteSpiException.class,
            "Node is not authorized to join as a server node"
        );

        assertEquals(1, grid(0).cluster().nodes().size());

        startGrid(configuration(1, JOIN_AS_SERVER));

        assertEquals(2, grid(0).cluster().nodes().size());

        // Client node can join cluster without any permissions granted.
        startClientGrid(configuration(2));

        assertEquals(3, grid(0).cluster().nodes().size());

        // Client node can reconnect without any permissions granted.
        CountDownLatch clientNodeReconnectedLatch = new CountDownLatch(1);

        grid(2).events().localListen(evt -> {
            clientNodeReconnectedLatch.countDown();

            return true;
        }, EVT_CLIENT_NODE_RECONNECTED);

        ignite(0).context().discovery().failNode(nodeId(2), null);

        assertTrue(clientNodeReconnectedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        assertEquals(3, grid(0).cluster().nodes().size());
    }

    /** */
    private static class SecurityPluginProvider extends TestSecurityPluginProvider {
        /** */
        public SecurityPluginProvider(
            String login,
            String pwd,
            SecurityPermissionSet perms,
            boolean globalAuth,
            TestSecurityData... clientData
        ) {
            super(login, pwd, perms, globalAuth, clientData);
        }

        /** {@inheritDoc} */
        @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
            return new SecurityProcessor(
                ctx,
                new TestSecurityData(login, pwd, perms, sandboxPerms),
                Arrays.asList(clientData),
                globalAuth
            );
        }
    }

    /**
     * Security Processor implementaiton that does not pass user security permissions to the Security Context and
     * expects all authorization checks to be delegated exclusively to {@link GridSecurityProcessor#authorize}.
     */
    private static class SecurityProcessor extends TestSecurityProcessor {
        /** */
        public SecurityProcessor(
            GridKernalContext ctx,
            TestSecurityData nodeSecData,
            Collection<TestSecurityData> predefinedAuthData,
            boolean globalAuth
        ) {
            super(ctx, nodeSecData, predefinedAuthData, globalAuth);
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
            TestSecurityData data = USERS.get(cred.getLogin());

            if (data == null || !Objects.equals(cred, data.credentials()))
                return null;

            SecurityContext res = new TestSecurityContext(
                new TestSecuritySubject()
                    .setType(REMOTE_NODE)
                    .setId(node.id())
                    .setAddr(new InetSocketAddress(F.first(node.addresses()), 0))
                    .setLogin(cred.getLogin())
                    .sandboxPermissions(data.sandboxPermissions())
            );

            SECURITY_CONTEXTS.put(res.subject().id(), res);

            return res;
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticate(AuthenticationContext ctx) {
            TestSecurityData data = USERS.get(ctx.credentials().getLogin());

            if (data == null || !Objects.equals(ctx.credentials(), data.credentials()))
                return null;

            SecurityContext res = new TestSecurityContext(
                new TestSecuritySubject()
                    .setType(ctx.subjectType())
                    .setId(ctx.subjectId())
                    .setAddr(ctx.address())
                    .setLogin(ctx.credentials().getLogin())
                    .setCerts(ctx.certificates())
                    .sandboxPermissions(data.sandboxPermissions())
            );

            SECURITY_CONTEXTS.put(res.subject().id(), res);

            return res;
        }

        /** {@inheritDoc} */
        @Override public void authorize(
            String name,
            SecurityPermission perm,
            SecurityContext securityCtx
        ) throws SecurityException {
            TestSecurityData userData = USERS.get(securityCtx.subject().login());

            if (userData == null || !contains(userData.permissions(), name, perm)) {
                throw new SecurityException("Authorization failed [perm=" + perm +
                    ", name=" + name +
                    ", subject=" + securityCtx.subject() + ']');
            }
        }

        /** */
        public static boolean contains(SecurityPermissionSet userPerms, String name, SecurityPermission perm) {
            boolean dfltAllowAll = userPerms.defaultAllowAll();

            switch (perm) {
                case CACHE_PUT:
                case CACHE_READ:
                case CACHE_REMOVE:
                    return contains(userPerms.cachePermissions(), dfltAllowAll, name, perm);

                case CACHE_CREATE:
                case CACHE_DESTROY:
                    return (name != null && contains(userPerms.cachePermissions(), dfltAllowAll, name, perm))
                        || containsSystemPermission(userPerms, perm);

                case TASK_CANCEL:
                case TASK_EXECUTE:
                    return contains(userPerms.taskPermissions(), dfltAllowAll, name, perm);

                case SERVICE_DEPLOY:
                case SERVICE_INVOKE:
                case SERVICE_CANCEL:
                    return contains(userPerms.servicePermissions(), dfltAllowAll, name, perm);

                default:
                    return containsSystemPermission(userPerms, perm);
            }
        }

        /** */
        private static boolean contains(
            Map<String, Collection<SecurityPermission>> userPerms,
            boolean dfltAllowAll,
            String name,
            SecurityPermission perm
        ) {
            Collection<SecurityPermission> perms = userPerms.get(name);

            if (perms == null)
                return dfltAllowAll;

            return perms.stream().anyMatch(perm::equals);
        }

        /** */
        private static boolean containsSystemPermission(
            SecurityPermissionSet userPerms,
            SecurityPermission perm
        ) {
            Collection<SecurityPermission> sysPerms = userPerms.systemPermissions();

            if (F.isEmpty(sysPerms))
                return userPerms.defaultAllowAll();

            return sysPerms.stream().anyMatch(perm::equals);
        }
    }

    /** */
    private static class TestSecurityContext implements SecurityContext, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final SecuritySubject subj;

        /** */
        public TestSecurityContext(SecuritySubject subj) {
            this.subj = subj;
        }

        /** {@inheritDoc} */
        @Override public SecuritySubject subject() {
            return subj;
        }

        /** {@inheritDoc} */
        @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean systemOperationAllowed(SecurityPermission perm) {
            return false;
        }
    }
}
