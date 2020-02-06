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

package org.apache.ignite.internal.processors.security.impl;

import java.net.InetSocketAddress;
import java.security.Permissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_NODE;

/**
 * Security processor for test.
 */
public class TestSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** V2 security subject for authenticated node. */
    public static final String ATTR_SECURITY_CONTEXT = IgniteNodeAttributes.ATTR_PREFIX + ".security.context";

    /** Permissions. */
    private static final Map<SecurityCredentials, SecurityPermissionSet> PERMS = new ConcurrentHashMap<>();

    /** Sandbox permissions. */
    private static final Map<SecurityCredentials, Permissions> SANDBOX_PERMS = new ConcurrentHashMap<>();

    /** Node security data. */
    private final TestSecurityData nodeSecData;

    /** Users security data. */
    private final Collection<TestSecurityData> predefinedAuthData;

    /** Global authentication. */
    private final boolean globalAuth;

    /**
     * Constructor.
     */
    public TestSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
        Collection<TestSecurityData> predefinedAuthData, boolean globalAuth) {
        super(ctx);

        this.nodeSecData = nodeSecData;
        this.predefinedAuthData = predefinedAuthData.isEmpty()
            ? Collections.emptyList()
            : new ArrayList<>(predefinedAuthData);
        this.globalAuth = globalAuth;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
        if (!PERMS.containsKey(cred))
            return null;

        SecurityContext res = new TestSecurityContext(
            new TestSecuritySubject()
                .setType(REMOTE_NODE)
                .setId(node.id())
                .setAddr(new InetSocketAddress(F.first(node.addresses()), 0))
                .setLogin(cred.getLogin())
                .setPerms(PERMS.get(cred))
                .sandboxPermissions(SANDBOX_PERMS.get(cred))
        );

        try {
            Map<String, Object> attrs = new HashMap<>(node.attributes());

            attrs.put(ATTR_SECURITY_CONTEXT, U.marshal(ctx.marshallerContext().jdkMarshaller(), res));

            ((TcpDiscoveryNode)node).setAttributes(attrs);

            return res;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    @Override public SecurityContext securityContext(UUID subjId) {
        ClusterNode node = ctx.discovery().getInjectedDiscoverySpi().getNode(subjId);

        if(node == null)
            return null;

        byte[] subjBytes = node.attribute(ATTR_SECURITY_CONTEXT);

        if (subjBytes == null)
            throw new SecurityException("Security context isn't certain.");

        try {
            return U.unmarshal(ctx.marshallerContext().jdkMarshaller(), subjBytes, U.resolveClassLoader(ctx.config()));
        }
        catch (IgniteCheckedException e) {
            throw new SecurityException("Failed to get security context.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return globalAuth;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) {
        if (!PERMS.containsKey(ctx.credentials()))
            return null;

        return new TestSecurityContext(
            new TestSecuritySubject()
                .setType(ctx.subjectType())
                .setId(ctx.subjectId())
                .setAddr(ctx.address())
                .setLogin(ctx.credentials().getLogin())
                .setPerms(PERMS.get(ctx.credentials()))
                .sandboxPermissions(SANDBOX_PERMS.get(ctx.credentials()))
        );
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {
        if (!((TestSecurityContext)securityCtx).operationAllowed(name, perm))
            throw new SecurityException("Authorization failed [perm=" + perm +
                ", name=" + name +
                ", subject=" + securityCtx.subject() + ']');
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        PERMS.put(nodeSecData.credentials(), nodeSecData.getPermissions());
        SANDBOX_PERMS.put(nodeSecData.credentials(), nodeSecData.sandboxPermissions());

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, nodeSecData.credentials());

        for (TestSecurityData data : predefinedAuthData) {
            PERMS.put(data.credentials(), data.getPermissions());
            SANDBOX_PERMS.put(nodeSecData.credentials(), data.sandboxPermissions());
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        PERMS.remove(nodeSecData.credentials());
        SANDBOX_PERMS.remove(nodeSecData.credentials());

        for (TestSecurityData data : predefinedAuthData) {
            PERMS.remove(data.credentials());
            SANDBOX_PERMS.remove(data.credentials());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean sandboxEnabled() {
        return true;
    }

    public static class TestSecurityProcessorDelegator extends GridProcessorAdapter implements GridSecurityProcessor {
        private final GridSecurityProcessor original;

        public TestSecurityProcessorDelegator(GridKernalContext ctx,
            GridSecurityProcessor original) {
            super(ctx);

            this.original = original;
        }

        @Override public SecurityContext authenticateNode(ClusterNode node,
            SecurityCredentials cred) throws IgniteCheckedException {
            return original.authenticateNode(node, cred);
        }

        @Override public SecurityContext securityContext(UUID subjId) {
            return original.securityContext(subjId);
        }

        @Override public void authorize(String name, SecurityPermission perm,
            SecurityContext securityCtx) throws SecurityException {
            original.authorize(name, perm, securityCtx);
        }

        @Override public boolean isGlobalNodeAuthentication() {
            return original.isGlobalNodeAuthentication();
        }

        @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
            return original.authenticate(ctx);
        }

        @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
            return original.authenticatedSubjects();
        }

        @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
            return original.authenticatedSubject(subjId);
        }

        @Override public void onSessionExpired(UUID subjId) {
            original.onSessionExpired(subjId);
        }

        @Override @Deprecated public boolean enabled() {
            return original.enabled();
        }

        @Override public boolean sandboxEnabled() {
            return original.sandboxEnabled();
        }

        @Override public void start() throws IgniteCheckedException {
            original.start();
        }

        @Override public void stop(boolean cancel) throws IgniteCheckedException {
            original.stop(cancel);
        }

        @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
            original.onKernalStart(active);
        }

        @Override public void onKernalStop(boolean cancel) {
            original.onKernalStop(cancel);
        }

        @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
            original.collectJoiningNodeData(dataBag);
        }

        @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
            original.collectGridNodeData(dataBag);
        }

        @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
            original.onGridDataReceived(data);
        }

        @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
            original.onJoiningNodeDataReceived(data);
        }

        @Override public void printMemoryStats() {
            original.printMemoryStats();
        }

        @Override public IgniteNodeValidationResult validateNode(
            ClusterNode node) {
            return original.validateNode(node);
        }

        @Override public IgniteNodeValidationResult validateNode(
            ClusterNode node, DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
            return original.validateNode(node, discoData);
        }

        @Override @Nullable public DiscoveryDataExchangeType discoveryDataType() {
            return original.discoveryDataType();
        }

        @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
            original.onDisconnected(reconnectFut);
        }

        @Override public IgniteInternalFuture<?> onReconnected(
            boolean clusterRestarted) throws IgniteCheckedException {
            return original.onReconnected(clusterRestarted);
        }
    }

}
