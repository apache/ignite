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

package org.apache.ignite.internal.processors.service;

import java.util.Collection;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Batch of services cluster deployment result.
 * <p/>
 * Contains collection of {@link ServiceClusterDeploymentResult}.
 */
public class ServiceClusterDeploymentResultBatch implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Deployment process id. */
    private final ServiceDeploymentProcessId depId;

    /** Services deployments results. */
    @GridToStringInclude
    private Collection<ServiceClusterDeploymentResult> results;

    /** Services deployment actions to be processed on services deployment process. */
    @GridToStringExclude
    @Nullable private transient ServiceDeploymentActions serviceDeploymentActions;

    /**
     * @param depId Deployment process id.
     * @param results Services deployments results.
     */
    public ServiceClusterDeploymentResultBatch(@NotNull ServiceDeploymentProcessId depId,
        @NotNull Collection<ServiceClusterDeploymentResult> results) {
        this.depId = depId;
        this.results = results;
    }

    /**
     * @return Deployment process id.
     */
    public ServiceDeploymentProcessId deploymentId() {
        return depId;
    }

    /**
     * @return Services deployments results.
     */
    public Collection<ServiceClusterDeploymentResult> results() {
        return results;
    }

    /**
     * @return Services deployment actions to be processed on services deployment process.
     */
    @Nullable public ServiceDeploymentActions servicesDeploymentActions() {
        return serviceDeploymentActions;
    }

    /**
     * @param serviceDeploymentActions Services deployment actions to be processed on services deployment process.
     */
    public void servicesDeploymentActions(ServiceDeploymentActions serviceDeploymentActions) {
        this.serviceDeploymentActions = serviceDeploymentActions;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceClusterDeploymentResultBatch.class, this);
    }
}
