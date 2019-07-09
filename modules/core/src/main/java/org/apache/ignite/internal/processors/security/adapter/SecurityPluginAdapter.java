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

package org.apache.ignite.internal.processors.security.adapter;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.SecurityPlugin;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.IgniteSecurityContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

public class SecurityPluginAdapter implements SecurityPlugin {

    private final GridSecurityProcessor prc;

    public SecurityPluginAdapter(GridSecurityProcessor prc) {
        this.prc = Objects.requireNonNull(prc);
    }

    @Override public IgniteSecurityContext authenticateNode(ClusterNode node,
        SecurityCredentials cred) throws IgniteCheckedException {
        SecurityContext sc = prc.authenticateNode(node, cred);

        return sc != null ? new IgniteSecurityContextAdapter(sc) : null;
    }

    @Override public boolean isGlobalNodeAuthentication() {
        return prc.isGlobalNodeAuthentication();
    }

    @Override public IgniteSecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        SecurityContext sc = prc.authenticate(ctx);

        return sc != null ? new IgniteSecurityContextAdapter(sc) : null;
    }

    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return prc.authenticatedSubjects();
    }

    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return prc.authenticatedSubject(subjId);
    }

    @Override public void onSessionExpired(UUID subjId) {
        prc.onSessionExpired(subjId);
    }

    @Override public void start() throws IgniteCheckedException {
        prc.start();
    }

    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        prc.stop(cancel);
    }

    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        prc.onKernalStart(active);
    }

    @Override public void onKernalStop(boolean cancel) {
        prc.onKernalStop(cancel);
    }

    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        prc.collectJoiningNodeData(dataBag);
    }

    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        prc.collectGridNodeData(dataBag);
    }

    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        prc.onGridDataReceived(data);
    }

    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        prc.onJoiningNodeDataReceived(data);
    }

    @Override public void printMemoryStats() {
        prc.printMemoryStats();
    }

    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        return prc.validateNode(node);
    }

    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        return prc.validateNode(node, discoData);
    }

    @Override public @Nullable DiscoveryDataExchangeType discoveryDataType() {
        return prc.discoveryDataType();
    }

    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        prc.onDisconnected(reconnectFut);
    }

    @Override
    public @Nullable IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        return prc.onReconnected(clusterRestarted);
    }
}
