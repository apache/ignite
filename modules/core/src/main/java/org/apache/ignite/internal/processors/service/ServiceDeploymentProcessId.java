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

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Service deployment process' identifier.
 */
public class ServiceDeploymentProcessId implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    @Order(value = 0, method = "topologyVersion")
    @Nullable private AffinityTopologyVersion topVer;

    /** Request's id. */
    @Order(value = 1, method = "requestId")
    @Nullable private IgniteUuid reqId;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServiceDeploymentProcessId() {
    }

    /**
     * @param topVer Topology version.
     */
    ServiceDeploymentProcessId(@NotNull AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @param reqId Request's id.
     */
    ServiceDeploymentProcessId(@NotNull IgniteUuid reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Topology version.
     */
    public @Nullable AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version.
     */
    public void topologyVersion(@Nullable AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Requests id.
     */
    public @Nullable IgniteUuid requestId() {
        return reqId;
    }

    /**
     * @param reqId Request's id.
     */
    public void requestId(@Nullable IgniteUuid reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 167;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServiceDeploymentProcessId id = (ServiceDeploymentProcessId)o;

        return Objects.equals(topVer, id.topVer) && Objects.equals(reqId, id.reqId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(topVer, reqId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceDeploymentProcessId.class, this);
    }
}
