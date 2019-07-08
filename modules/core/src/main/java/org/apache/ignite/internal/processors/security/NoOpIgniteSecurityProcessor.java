/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.security.IgniteSecurityProcessor.ATTR_GRID_SEC_PROC_CLASS;

/**
 * No operation IgniteSecurity.
 */
public class NoOpIgniteSecurityProcessor extends GridProcessorAdapter implements IgniteSecurity {
    /** No operation security context. */
    private final OperationSecurityContext opSecCtx = new OperationSecurityContext(this, null);

    /** Processor delegate. */
    private final GridSecurityProcessor processor;

    /**
     * @param ctx Grid kernal context.
     */
    public NoOpIgniteSecurityProcessor(GridKernalContext ctx, @Nullable GridSecurityProcessor processor) {
        super(ctx);
        this.processor =processor;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        if(processor != null)
            processor.onKernalStart(active);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if(processor != null)
            processor.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        if (processor != null) {
            ctx.addNodeAttribute(ATTR_GRID_SEC_PROC_CLASS, processor.getClass().getName());

            processor.start();
        }
        else
            ctx.addNodeAttribute(ATTR_GRID_SEC_PROC_CLASS, getClass().getName());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if(processor != null)
            processor.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(SecurityContext secCtx) {
        return opSecCtx;
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(UUID nodeId) {
        return opSecCtx;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm) throws SecurityException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        if(processor != null)
            return processor.enabled();

        return false;
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null || processor == null ? res : processor.validateNode(node);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        IgniteNodeValidationResult res = validateSecProcClass(node);

        return res != null || processor == null ? res : processor.validateNode(node, discoData);
    }

    /**
     * Validates that remote the node's grid security processor class is undefined.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    private IgniteNodeValidationResult validateSecProcClass(ClusterNode node){
        String rmtCls = node.attribute(ATTR_GRID_SEC_PROC_CLASS);

        boolean securityMsgSupported = IgniteFeatures.allNodesSupports(
            ctx,
            ctx.discovery().allNodes(),
            IgniteFeatures.IGNITE_SECURITY_PROCESSOR
        );

        if(securityMsgSupported && processor != null) {
            String locCls = processor.getClass().getName();

            // Compatibility. It allows connect an old node to a new cluster.
            if (!processor.enabled() && rmtCls == null)
                return null;

            if (!F.eq(locCls, rmtCls) && !F.eq(getClass().getName(), rmtCls)) {
                return new IgniteNodeValidationResult(node.id(),
                    String.format(MSG_SEC_PROC_CLS_IS_INVALID, ctx.localNodeId(), node.id(), locCls, rmtCls),
                    String.format(MSG_SEC_PROC_CLS_IS_INVALID, node.id(), ctx.localNodeId(), rmtCls, locCls));
            }

            return null;
        }

        if (securityMsgSupported && rmtCls != null && !rmtCls.equals(getClass().getName())) {
            ClusterNode locNode = ctx.discovery().localNode();

            return new IgniteNodeValidationResult(
                node.id(),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, locNode.id(), node.id(), "undefined", rmtCls),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, node.id(), locNode.id(), rmtCls, "undefined")
            );
        }

        return null;
    }

    /** */
    public GridSecurityProcessor gridSecurityProcessor() {
        return processor;
    }
}
