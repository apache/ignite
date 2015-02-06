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

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * User message wrapper.
 */
public class GridIoUserMessage extends GridTcpCommunicationMessageAdapter {
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
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridIoUserMessage _clone = new GridIoUserMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridIoUserMessage _clone = (GridIoUserMessage)_msg;

        _clone.body = body;
        _clone.bodyBytes = bodyBytes;
        _clone.clsLdrId = clsLdrId;
        _clone.topic = topic;
        _clone.topicBytes = topicBytes;
        _clone.depMode = depMode;
        _clone.depClsName = depClsName;
        _clone.userVer = userVer;
        _clone.ldrParties = ldrParties;
        _clone.dep = dep;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray("bodyBytes", bodyBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putGridUuid("clsLdrId", clsLdrId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putString("depClsName", depClsName))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putEnum("depMode", depMode))
                    return false;

                commState.idx++;

            case 4:
                if (ldrParties != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, ldrParties.size()))
                            return false;

                        commState.it = ldrParties.entrySet().iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        Map.Entry<UUID, IgniteUuid> e = (Map.Entry<UUID, IgniteUuid>)commState.cur;

                        if (!commState.keyDone) {
                            if (!commState.putUuid(null, e.getKey()))
                                return false;

                            commState.keyDone = true;
                        }

                        if (!commState.putGridUuid(null, e.getValue()))
                            return false;

                        commState.keyDone = false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 5:
                if (!commState.putByteArray("topicBytes", topicBytes))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putString("userVer", userVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                bodyBytes = commState.getByteArray("bodyBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                clsLdrId = commState.getGridUuid("clsLdrId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 2:
                depClsName = commState.getString("depClsName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 3:
                byte depMode0 = commState.getByte("depMode");

                if (!commState.lastRead())
                    return false;

                depMode = DeploymentMode.fromOrdinal(depMode0);

                commState.idx++;

            case 4:
                if (commState.readSize == -1) {
                    int _val = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                    commState.readSize = _val;
                }

                if (commState.readSize >= 0) {
                    if (ldrParties == null)
                        ldrParties = new HashMap<>(commState.readSize, 1.0f);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (!commState.keyDone) {
                            UUID _val = commState.getUuid(null);

                            if (!commState.lastRead())
                                return false;

                            commState.cur = _val;
                            commState.keyDone = true;
                        }

                        IgniteUuid _val = commState.getGridUuid(null);

                        if (!commState.lastRead())
                            return false;

                        ldrParties.put((UUID)commState.cur, _val);

                        commState.keyDone = false;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;
                commState.cur = null;

                commState.idx++;

            case 5:
                topicBytes = commState.getByteArray("topicBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 6:
                userVer = commState.getString("userVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 9;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIoUserMessage.class, this);
    }
}
