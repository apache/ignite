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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Deployment request.
 */
public class GridDeploymentRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Response topic. Response should be sent back to this topic. */
    @GridDirectTransient
    private Object resTopic;

    /** Serialized topic. */
    private byte[] resTopicBytes;

    /** Requested class name. */
    private String rsrcName;

    /** Class loader ID. */
    private IgniteUuid ldrId;

    /** Undeploy flag. */
    private boolean isUndeploy;

    /** Nodes participating in request (chain). */
    @GridToStringInclude
    @GridDirectCollection(UUID.class)
    private Collection<UUID> nodeIds;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
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
     * @param resTopic Response topic.
     */
    void responseTopic(Object resTopic) {
        this.resTopic = resTopic;
    }

    /**
     * @return Serialized topic.
     */
    byte[] responseTopicBytes() {
        return resTopicBytes;
    }

    /**
     * @param resTopicBytes Serialized topic.
     */
    void responseTopicBytes(byte[] resTopicBytes) {
        this.resTopicBytes = resTopicBytes;
    }

    /**
     * Class name/resource name that is being requested.
     *
     * @return Resource or class name.
     */
    String resourceName() {
        return rsrcName;
    }

    /**
     * Gets property ldrId.
     *
     * @return Property ldrId.
     */
    IgniteUuid classLoaderId() {
        return ldrId;
    }

    /**
     * Gets property undeploy.
     *
     * @return Property undeploy.
     */
    boolean isUndeploy() {
        return isUndeploy;
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeBoolean("isUndeploy", isUndeploy))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("ldrId", ldrId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection("nodeIds", nodeIds, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("resTopicBytes", resTopicBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeString("rsrcName", rsrcName))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                isUndeploy = reader.readBoolean("isUndeploy");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ldrId = reader.readIgniteUuid("ldrId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                nodeIds = reader.readCollection("nodeIds", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                resTopicBytes = reader.readByteArray("resTopicBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                rsrcName = reader.readString("rsrcName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDeploymentRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 11;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentRequest.class, this);
    }
}