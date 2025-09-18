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

import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.NotNull;

/**
 * Batch of service single node deployment result.
 * <p/>
 * Contains collection of {@link ServiceSingleNodeDeploymentResult} mapped services ids.
 */
public class ServiceSingleNodeDeploymentResultBatch implements Message {
    /** Deployment process id. */
    @Order(value = 0, method = "deploymentId")
    @GridToStringInclude
    private ServiceDeploymentProcessId depId;

    /** Services deployments results. */
    @Order(1)
    @GridToStringInclude
    private Map<IgniteUuid, ServiceSingleNodeDeploymentResult> results;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServiceSingleNodeDeploymentResultBatch() {
    }

    /**
     * @param depId Deployment process id.
     * @param results Services deployments results.
     */
    public ServiceSingleNodeDeploymentResultBatch(@NotNull ServiceDeploymentProcessId depId,
        @NotNull Map<IgniteUuid, ServiceSingleNodeDeploymentResult> results) {
        this.depId = depId;
        this.results = results;
    }

    /**
     * @return Services deployments results.
     */
    public Map<IgniteUuid, ServiceSingleNodeDeploymentResult> results() {
        return results;
    }

    /**
     * @param results Services deployments results.
     */
    public void results(Map<IgniteUuid, ServiceSingleNodeDeploymentResult> results) {
        this.results = results;
    }

    /**
     * @return Deployment process id.
     */
    public ServiceDeploymentProcessId deploymentId() {
        return depId;
    }

    /**
     * @param depId Deployment process id.
     */
    public void deploymentId(ServiceDeploymentProcessId depId) {
        this.depId = depId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 168;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceSingleNodeDeploymentResultBatch.class, this);
    }
}
