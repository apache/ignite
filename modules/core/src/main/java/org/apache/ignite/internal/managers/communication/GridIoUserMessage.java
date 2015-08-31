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

package org.apache.ignite.internal.managers.communication;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * User message wrapper.
 */
public class GridIoUserMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message body. */
    @GridDirectTransient
    private Object body;

    /** Serialized message body. */
    private byte[] bodyBytes;

    /** Class loader ID. */
    private IgniteUuid clsLdrId;

    /** Message topic. */
    @GridDirectTransient
    private Object topic;

    /** Serialized message topic. */
    private byte[] topicBytes;

    /** Deployment mode. */
    private DeploymentMode depMode;

    /** Deployment class name. */
    private String depClsName;

    /** User version. */
    private String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = IgniteUuid.class)
    private Map<UUID, IgniteUuid> ldrParties;

    /** Message deployment. */
    @GridDirectTransient
    private GridDeployment dep;

    /**
     * @param body Message body.
     * @param bodyBytes Serialized message body.
     * @param depClsName Message body class name.
     * @param topic Message topic.
     * @param topicBytes Serialized message topic bytes.
     * @param clsLdrId Class loader ID.
     * @param depMode Deployment mode.
     * @param userVer User version.
     * @param ldrParties Node loader participant map.
     */
    GridIoUserMessage(
        Object body,
        @Nullable byte[] bodyBytes,
        @Nullable String depClsName,
        @Nullable Object topic,
        @Nullable byte[] topicBytes,
        @Nullable IgniteUuid clsLdrId,
        @Nullable DeploymentMode depMode,
        @Nullable String userVer,
        @Nullable Map<UUID, IgniteUuid> ldrParties) {
        this.body = body;
        this.bodyBytes = bodyBytes;
        this.depClsName = depClsName;
        this.topic = topic;
        this.topicBytes = topicBytes;
        this.depMode = depMode;
        this.clsLdrId = clsLdrId;
        this.userVer = userVer;
        this.ldrParties = ldrParties;
    }

    /**
     * Default constructor, required for {@link Externalizable}.
     */
    public GridIoUserMessage() {
        // No-op.
    }

    /**
     * @return Serialized message body.
     */
    @Nullable public byte[] bodyBytes() {
        return bodyBytes;
    }

    /**
     * @return the Class loader ID.
     */
    @Nullable public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    @Nullable public DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Message body class name.
     */
    @Nullable public String deploymentClassName() {
        return depClsName;
    }

    /**
     * @return User version.
     */
    @Nullable public String userVersion() {
        return userVer;
    }

    /**
     * @return Node class loader participant map.
     */
    @Nullable public Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParties != null ? Collections.unmodifiableMap(ldrParties) : null;
    }

    /**
     * @return Serialized message topic.
     */
    @Nullable public byte[] topicBytes() {
        return topicBytes;
    }

    /**
     * @param topic New message topic.
     */
    public void topic(Object topic) {
        this.topic = topic;
    }

    /**
     * @return Message topic.
     */
    @Nullable public Object topic() {
        return topic;
    }

    /**
     * @param body New message body.
     */
    public void body(Object body) {
        this.body = body;
    }

    /**
     * @return Message body.
     */
    @Nullable public Object body() {
        return body;
    }

    /**
     * @param dep New message deployment.
     */
    public void deployment(GridDeployment dep) {
        this.dep = dep;
    }

    /**
     * @return Message deployment.
     */
    @Nullable public GridDeployment deployment() {
        return dep;
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
                if (!writer.writeByteArray("bodyBytes", bodyBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("clsLdrId", clsLdrId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString("depClsName", depClsName))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByte("depMode", depMode != null ? (byte)depMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap("ldrParties", ldrParties, MessageCollectionItemType.UUID, MessageCollectionItemType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("topicBytes", topicBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeString("userVer", userVer))
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
                bodyBytes = reader.readByteArray("bodyBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                clsLdrId = reader.readIgniteUuid("clsLdrId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                depClsName = reader.readString("depClsName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                byte depModeOrd;

                depModeOrd = reader.readByte("depMode");

                if (!reader.isLastRead())
                    return false;

                depMode = DeploymentMode.fromOrdinal(depModeOrd);

                reader.incrementState();

            case 4:
                ldrParties = reader.readMap("ldrParties", MessageCollectionItemType.UUID, MessageCollectionItemType.IGNITE_UUID, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                topicBytes = reader.readByteArray("topicBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                userVer = reader.readString("userVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridIoUserMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 9;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIoUserMessage.class, this);
    }
}