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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import javax.security.auth.Subject;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.IgniteSecurityContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.security.SecurityUtils.nodeSecurityContext;

/**
 * Default IgniteSecurity implementation.
 */
public class IgniteSecurityProcessor implements IgniteSecurity, GridProcessor {

    private static final ProtectionDomain[] NULL_PD_ARRAY = new ProtectionDomain[0];

    /** Internal attribute name constant. */
    public static final String ATTR_GRID_SEC_PROC_CLASS = "grid.security.processor.class";

    /** Current security context. */
    private final ThreadLocal<IgniteSecurityContext> curSecCtx = ThreadLocal.withInitial(this::localSecurityContext);

    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /** Security plugin. */
    private final SecurityPlugin secPlg;

    /** Must use JDK marshaller for Security Subject. */
    private final JdkMarshaller marsh;

    /** Map of security contexts. Key is the node's id. */
    private final Map<UUID, IgniteSecurityContext> secCtxs = new ConcurrentHashMap<>();

    /**
     * @param ctx Grid kernal context.
     * @param secPlg Security processor.
     */
    public IgniteSecurityProcessor(GridKernalContext ctx, SecurityPlugin secPlg) {
        this.ctx = Objects.requireNonNull(ctx);
        this.secPlg = Objects.requireNonNull(secPlg);

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(IgniteSecurityContext secCtx) {
        assert secCtx != null;

        IgniteSecurityContext old = curSecCtx.get();

        curSecCtx.set(secCtx);

        return new OperationSecurityContext(this, old);
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(UUID nodeId) {
        return withContext(
            secCtxs.computeIfAbsent(nodeId,
                uuid -> nodeSecurityContext(
                    marsh, U.resolveClassLoader(ctx.config()), ctx.discovery().node(uuid)
                )
            )
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteSecurityContext securityContext() {
        IgniteSecurityContext res = curSecCtx.get();

        assert res != null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteSecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        return secPlg.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return secPlg.isGlobalNodeAuthentication();
    }

    /** {@inheritDoc} */
    @Override public IgniteSecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return secPlg.authenticate(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return secPlg.authenticatedSubjects();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return secPlg.authenticatedSubject(subjId);
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        secPlg.onSessionExpired(subjId);
    }

    @Override public void checkPermission(Permission perm) throws SecurityException {
        Objects.requireNonNull(perm);

        IgniteSecurityContext secCtx = curSecCtx.get();

        assert secCtx != null;

        if (!secCtx.permissions().implies(perm))
            throw new SecurityException("Authorization is failed [perm=" + perm + ']');
    }

    @Override public <T> T execute(Callable<T> c) throws Exception {
        Objects.requireNonNull(c);

        final IgniteSecurityContext secCtx = curSecCtx.get();

        assert secCtx != null;

        final Subject subject = new Subject(true,
            Collections.singleton(new IgnitePrincipal(secCtx.subject().login().toString())),
            Collections.emptySet(), Collections.emptySet()
        );

        final AccessControlContext acc = AccessController.doPrivileged
            (new java.security.PrivilegedAction<AccessControlContext>() {
                @Override public AccessControlContext run() {
                    return new AccessControlContext
                        (new AccessControlContext(NULL_PD_ARRAY),
                            new IgniteSubjectDomainCombainer(subject, secCtx.permissions()));
                }
            });

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>)c::call, acc);
        }
        catch (PrivilegedActionException pae) {
            throw pae.getException();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_GRID_SEC_PROC_CLASS, secPlg.getClass().getName());

        secPlg.start();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        secPlg.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(
            (evt, discoCache) -> secCtxs.remove(evt.eventNode().id()), EVT_NODE_FAILED, EVT_NODE_LEFT
        );

        secPlg.onKernalStart(active);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        secPlg.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        secPlg.collectJoiningNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        secPlg.collectGridNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        secPlg.onGridDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        secPlg.onJoiningNodeDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        secPlg.printMemoryStats();
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPlg.validateNode(node);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null ? res : secPlg.validateNode(node, discoData);
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoveryDataExchangeType discoveryDataType() {
        return secPlg.discoveryDataType();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        secPlg.onDisconnected(reconnectFut);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteInternalFuture<?> onReconnected(
        boolean clusterRestarted) throws IgniteCheckedException {
        return secPlg.onReconnected(clusterRestarted);
    }

    /**
     * Getting local node's security context.
     *
     * @return Security context of local node.
     */
    private IgniteSecurityContext localSecurityContext() {
        return nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), ctx.discovery().localNode());
    }

    /**
     * Validates that remote node's grid security processor class is the same as local one.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    private IgniteNodeValidationResult validateSecProcClass(ClusterNode node) {
        //todo MY_TODO валидаию нужно делать с учетом версии плагина безопасности, а не только по его классу
        String rmtCls = node.attribute(ATTR_GRID_SEC_PROC_CLASS);
        String locCls = secPlg.getClass().getName();

        if (!F.eq(locCls, rmtCls)) {
            return new IgniteNodeValidationResult(node.id(),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, ctx.localNodeId(), node.id(), locCls, rmtCls),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, node.id(), ctx.localNodeId(), rmtCls, locCls));
        }

        return null;
    }
}
