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
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Deployment request.
 */
public class GridDeploymentRequest implements Message {
    /** Response topic. Response should be sent back to this topic. */
    /** */
    @Order(0)
    @Nullable GridTopic topic;

    /** */
    @Order(1)
    @Nullable IgniteUuid topicId;

    /** Requested class name. */
    @Order(2)
    String rsrcName;

    /** Class loader ID. */
    @Order(3)
    @Nullable IgniteUuid ldrId;

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
     * Creates deploy request.
     *
     * @param topic Response topic.
     * @param ldrId Class loader ID.
     * @param rsrcName Resource name that should be found and sent back.
     */
    GridDeploymentRequest(GridTopic.T1 topic, IgniteUuid ldrId, String rsrcName) {
        this.topic = topic.topic();
        topicId = topic.id();
        this.ldrId = ldrId;
        this.rsrcName = rsrcName;
    }

    /**
     * Creates undeploy request.
     *
     * @param rsrcName Resource name that should be found and sent back.
     */
    GridDeploymentRequest(String rsrcName) {
        this.rsrcName = rsrcName;
    }

    /**
     * Get topic response should be sent to.
     *
     * @return Response topic name.
     */
    @Nullable GridTopic.T1 responseTopic() {
        assert topic == null && topicId == null || topic != null && topicId != null;

        return topic == null ? null : new GridTopic.T1(topic, topicId);
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
     * Gets property ldrId.
     *
     * @return Property class loader ID.
     */
    public @Nullable IgniteUuid classLoaderId() {
        return ldrId;
    }

    /**
     * Gets property undeploy.
     *
     * @return Property undeploy.
     */
    public boolean undeploy() {
        assert topic == null && topicId == null || topic != null && topicId != null;

        return topic == null;
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

    /** {@inheritDoc} */
    @Override public short directType() {
        return 11;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentRequest.class, this);
    }
}
