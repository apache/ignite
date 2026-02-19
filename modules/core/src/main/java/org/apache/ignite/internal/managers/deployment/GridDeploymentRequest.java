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

package org.apache.ignite.internal.managers.deployment;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Deployment request.
 */
public class GridDeploymentRequest implements Message {
    /** Response topic. Response should be sent back to this topic. */
    private Object resTopic;

    /** Serialized topic. */
    @Order(0)
    byte[] resTopicBytes;

    /** Requested class name. */
    @Order(1)
    String rsrcName;

    /** Class loader ID. */
    @Order(2)
    IgniteUuid ldrId;

    /** Undeploy flag. */
    @Order(3)
    boolean isUndeploy;

    /** Nodes participating in request (chain). */
    @Order(4)
    @GridToStringInclude
    Collection<UUID> nodeIds;

    /**
     * Default constructor.
     */
    public GridDeploymentRequest() {
        // No-op.
    }

    /**
     * Creates new request.
     *
     * @param resTopic Response topic.
     * @param ldrId Class loader ID.
     * @param rsrcName Resource name that should be found and sent back.
     * @param isUndeploy Undeploy property.
     */
    GridDeploymentRequest(Object resTopic, IgniteUuid ldrId, String rsrcName, boolean isUndeploy) {
        assert isUndeploy || resTopic != null;
        assert isUndeploy || ldrId != null;
        assert rsrcName != null;

        this.resTopic = resTopic;
        this.ldrId = ldrId;
        this.rsrcName = rsrcName;
        this.isUndeploy = isUndeploy;
    }

    /**
     * Get topic response should be sent to.
     *
     * @return Response topic name.
     */
    Object responseTopic() {
        return resTopic;
    }

    /**
     * @return Serialized topic.
     */
    public byte[] responseTopicBytes() {
        return resTopicBytes;
    }

    /**
     * @param resTopicBytes Serialized topic.
     */
    public void responseTopicBytes(byte[] resTopicBytes) {
        this.resTopicBytes = resTopicBytes;
    }

    /**
     * Class name/resource name that is being requested.
     *
     * @return Resource or class name.
     */
    public String resourceName() {
        return rsrcName;
    }

    /**
     * @param rsrcName Resource or class name.
     */
    public void resourceName(String rsrcName) {
        this.rsrcName = rsrcName;
    }

    /**
     * Gets property ldrId.
     *
     * @return Property class loader ID.
     */
    public IgniteUuid classLoaderId() {
        return ldrId;
    }

    /**
     * @param ldrId Property class loader ID.
     */
    public void classLoaderId(IgniteUuid ldrId) {
        this.ldrId = ldrId;
    }

    /**
     * Gets property undeploy.
     *
     * @return Property undeploy.
     */
    public boolean isUndeploy() {
        return isUndeploy;
    }

    /**
     * @param isUndeploy Property undeploy.
     */
    public void isUndeploy(boolean isUndeploy) {
        this.isUndeploy = isUndeploy;
    }

    /**
     * @return Node IDs chain which is updated as request jumps
     *      from node to node.
     */
    public Collection<UUID> nodeIds() {
        return nodeIds;
    }

    /**
     * @param nodeIds Node IDs chain which is updated as request jumps
     *      from node to node.
     */
    public void nodeIds(Collection<UUID> nodeIds) {
        this.nodeIds = nodeIds;
    }

    /**
     * @param marsh Marshaller.
     */
    public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (resTopic != null && resTopicBytes == null)
            resTopicBytes = U.marshal(marsh, resTopic);
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (resTopicBytes != null && resTopic == null) {
            resTopic = U.unmarshal(marsh, resTopicBytes, ldr);

            resTopicBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 11;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentRequest.class, this);
    }
}
