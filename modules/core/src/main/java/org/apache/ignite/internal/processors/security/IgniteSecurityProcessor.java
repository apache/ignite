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

import java.security.Security;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.security.sandbox.AccessControllerSandbox;
import org.apache.ignite.internal.processors.security.sandbox.IgniteSandbox;
import org.apache.ignite.internal.processors.security.sandbox.NoOpSandbox;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_ID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2;
import static org.apache.ignite.internal.processors.security.SecurityUtils.IGNITE_INTERNAL_PACKAGE;
import static org.apache.ignite.internal.processors.security.SecurityUtils.MSG_SEC_PROC_CLS_IS_INVALID;
import static org.apache.ignite.internal.processors.security.SecurityUtils.hasSecurityManager;

/**
 * Default IgniteSecurity implementation.
 */
public class IgniteSecurityProcessor implements IgniteSecurity, GridProcessor {
    /** Internal attribute name constant. */
    public static final String ATTR_GRID_SEC_PROC_CLASS = "grid.security.processor.class";

    /** Number of started nodes with the sandbox enabled. */
    private static final AtomicInteger SANDBOXED_NODES_COUNTER = new AtomicInteger();

    /** Current security context. */
    private final ThreadLocal<SecurityContext> curSecCtx = ThreadLocal.withInitial(this::localSecurityContext);

    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /** Security processor. */
    private final GridSecurityProcessor secPrc;

    /** Must use JDK marshaller for Security Subject. */
    private final JdkMarshaller marsh;

    /** Map of security contexts. Key is the node's id. */
    private final Map<UUID, SecurityContext> secCtxs = new ConcurrentHashMap<>();

    /** Instance of IgniteSandbox. */
    private IgniteSandbox sandbox;

    /**
     * @param ctx Grid kernal context.
     * @param secPrc Security processor.
     */
    public IgniteSecurityProcessor(GridKernalContext ctx, GridSecurityProcessor secPrc) {
        assert ctx != null;
        assert secPrc != null;

        this.ctx = ctx;
        this.secPrc = secPrc;

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(SecurityContext secCtx) {
        assert secCtx != null;

        SecurityContext old = curSecCtx.get();

        curSecCtx.set(secCtx);

        return new OperationSecurityContext(this, old);
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(UUID subjId) {
        return withContext(securityContext(subjId));
    }

    /**
     * Resolves cluster node by its ID.
     *
     * @param nodeId Node id.
     * @throws IllegalStateException If node with provided ID doesn't exist.
     */
    private ClusterNode findNode(UUID nodeId) {
        ClusterNode node = Optional.ofNullable(ctx.discovery().node(nodeId))
            .orElseGet(() -> ctx.discovery().historicalNode(nodeId));

        if (node == null)
            throw new IllegalStateException("Failed to find node with given ID for security context setup: " + nodeId);

        return node;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext() {
        SecurityContext res = curSecCtx.get();

        assert res != null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext(UUID subjId) {
        Objects.requireNonNull(subjId, "Parameter 'subjId' cannot be null.");

        return secCtxs.computeIfAbsent(subjId,
            id -> {
                SecurityContext res = secPrc.securityContext(id);

                if (res == null)
                    res = nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), findNode(id));

                return res;
            });
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext(ClusterNode node) {
        return securityContext(subjectId(node));
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        SecurityContext res = secPrc.authenticateNode(node, cred);

        if (res != null) {
            Map<String, Object> attrs = new HashMap<>(node.attributes());

            attrs.put(ATTR_SECURITY_SUBJECT_ID, res.subject().id());

            if (node instanceof IgniteClusterNode)
                ((IgniteClusterNode)node).setAttributes(attrs);
            else
                throw new IgniteCheckedException("Node must implement interface IgniteClusterNode.");
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return secPrc.isGlobalNodeAuthentication();
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return secPrc.authenticate(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return secPrc.authenticatedSubjects();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return secPrc.authenticatedSubject(subjId);
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        secCtxs.remove(subjId);
        secPrc.onSessionExpired(subjId);
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm) throws SecurityException {
        SecurityContext secCtx = curSecCtx.get();

        assert secCtx != null;

        secPrc.authorize(name, perm, secCtx);
    }

    /** {@inheritDoc} */
    @Override public IgniteSandbox sandbox() {
        return sandbox;
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_GRID_SEC_PROC_CLASS, secPrc.getClass().getName());

        secPrc.start();

        if (hasSecurityManager() && secPrc.sandboxEnabled()) {
            sandbox = new AccessControllerSandbox(this);

            updatePackageAccessProperty();
        }
        else {
            if (secPrc.sandboxEnabled()) {
                ctx.log(getClass()).warning("GridSecurityProcessor#sandboxEnabled returns true, " +
                    "but system SecurityManager is not defined, " +
                    "that may be a cause of security lack when IgniteCompute or IgniteCache operations perform.");
            }

            sandbox = new NoOpSandbox();
        }
    }

    /**
     * Updates the package access property to specify the internal Ignite package.
     */
    private void updatePackageAccessProperty() {
        synchronized (SANDBOXED_NODES_COUNTER) {
            if (SANDBOXED_NODES_COUNTER.getAndIncrement() == 0) {
                String packAccess = Security.getProperty("package.access");

                if (!F.isEmpty(packAccess)) {
                    if (!packAccess.contains(IGNITE_INTERNAL_PACKAGE))
                        Security.setProperty("package.access", packAccess + ',' + IGNITE_INTERNAL_PACKAGE);
                }
                else
                    Security.setProperty("package.access", IGNITE_INTERNAL_PACKAGE);
            }
        }
    }

    /**
     * Gets the node's security context.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @param node Node.
     * @return Node's security context.
     */
    private SecurityContext nodeSecurityContext(Marshaller marsh, ClassLoader ldr, ClusterNode node) {
        byte[] subjBytes = node.attribute(ATTR_SECURITY_SUBJECT_V2);

        if (subjBytes == null)
            throw new SecurityException("Security context isn't certain.");

        try {
            return U.unmarshal(marsh, subjBytes, ldr);
        }
        catch (IgniteCheckedException e) {
            throw new SecurityException("Failed to get security context.", e);
        }
    }

    /**
     * Gets security subject id associated with node.
     *
     * @param node Cluster node.
     * @return Security subject id.
     */
    private UUID subjectId(ClusterNode node) {
        return node.attribute(ATTR_SECURITY_SUBJECT_ID);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        clearPackageAccessProperty();

        secCtxs.remove(subjectId(ctx.discovery().localNode()));
        secPrc.stop(cancel);
    }

    /**
     *
     */
    private void clearPackageAccessProperty() {
        if (hasSecurityManager() && secPrc.sandboxEnabled()) {
            synchronized (SANDBOXED_NODES_COUNTER) {
                if (SANDBOXED_NODES_COUNTER.decrementAndGet() == 0) {
                    String packAccess = Security.getProperty("package.access");

                    if (packAccess.equals(IGNITE_INTERNAL_PACKAGE))
                        Security.setProperty("package.access", null);
                    else if (packAccess.contains(',' + IGNITE_INTERNAL_PACKAGE))
                        Security.setProperty("package.access", packAccess.replace(',' + IGNITE_INTERNAL_PACKAGE, ""));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(
            (evt, discoCache) -> secCtxs.remove(evt.eventNode().id()), EVT_NODE_FAILED, EVT_NODE_LEFT
        );

        secPrc.onKernalStart(active);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        secPrc.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        secPrc.collectJoiningNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        secPrc.collectGridNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        secPrc.onGridDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        secPrc.onJoiningNodeDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        secPrc.printMemoryStats();
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPrc.validateNode(node);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPrc.validateNode(node, discoData);
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoveryDataExchangeType discoveryDataType() {
        return secPrc.discoveryDataType();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        secPrc.onDisconnected(reconnectFut);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteInternalFuture<?> onReconnected(
        boolean clusterRestarted) throws IgniteCheckedException {
        return secPrc.onReconnected(clusterRestarted);
    }

    /**
     * Getting local node's security context.
     *
     * @return Security context of local node.
     */
    private SecurityContext localSecurityContext() {
        return securityContext(ctx.discovery().localNode());
    }

    /**
     * Validates that remote node's grid security processor class is the same as local one.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    private IgniteNodeValidationResult validateSecProcClass(ClusterNode node) {
        String rmtCls = node.attribute(ATTR_GRID_SEC_PROC_CLASS);
        String locCls = secPrc.getClass().getName();

        if (!F.eq(locCls, rmtCls)) {
            return new IgniteNodeValidationResult(node.id(),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, ctx.localNodeId(), node.id(), locCls, rmtCls),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, node.id(), ctx.localNodeId(), rmtCls, locCls));
        }

        return null;
    }
}
