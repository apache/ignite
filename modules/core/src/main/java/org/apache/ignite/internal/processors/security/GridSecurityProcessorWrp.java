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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper class for implementation of {@link GridSecurityProcessor}.
 */
public class GridSecurityProcessorWrp implements GridSecurityProcessor {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Original security processor. */
    protected final GridSecurityProcessor original;

    /**
     * @param ctx Grid kernal context.
     * @param original Original grid security processor.
     */
    public GridSecurityProcessorWrp(GridKernalContext ctx, GridSecurityProcessor original) {
        this.ctx = ctx;
        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node,
        SecurityCredentials cred) throws IgniteCheckedException {
        return original.authenticateNode(node, cred);
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return original.isGlobalNodeAuthentication();
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return original.authenticate(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return original.authenticatedSubjects();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return original.authenticatedSubject(subjId);
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {
        original.authorize(name, perm, securityCtx);
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        original.onSessionExpired(subjId);
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return original.enabled();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        original.start();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        original.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        original.onKernalStart(active);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        original.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        original.collectJoiningNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        original.collectGridNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        original.onGridDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        original.onJoiningNodeDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        original.printMemoryStats();
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteNodeValidationResult validateNode(ClusterNode node) {
        return original.validateNode(node);
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        return original.validateNode(node, discoData);
    }

    /** {@inheritDoc} */
    @Override @Nullable public DiscoveryDataExchangeType discoveryDataType() {
        return original.discoveryDataType();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        original.onDisconnected(reconnectFut);
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted)
        throws IgniteCheckedException {
        return original.onReconnected(clusterRestarted);
    }
}