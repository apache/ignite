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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Deployment info bean.
 */
public class GridDeploymentInfoBean implements Message, GridDeploymentInfo, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid clsLdrId;

    /** */
    private DeploymentMode depMode;

    /** */
    private String userVer;

    /** */
    private boolean locDepOwner;

    /** Node class loader participant map. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = IgniteUuid.class)
    private Map<UUID, IgniteUuid> participants;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDeploymentInfoBean() {
        /* No-op. */
    }

    /**
     * @param clsLdrId Class loader ID.
     * @param userVer User version.
     * @param depMode Deployment mode.
     * @param participants Participants.
     * @param locDepOwner Local deployment owner flag.
     */
    public GridDeploymentInfoBean(IgniteUuid clsLdrId, String userVer, DeploymentMode depMode,
        Map<UUID, IgniteUuid> participants, boolean locDepOwner) {
        this.clsLdrId = clsLdrId;
        this.depMode = depMode;
        this.userVer = userVer;
        this.participants = participants;
        this.locDepOwner = locDepOwner;
    }

    /**
     * @param dep Grid deployment.
     */
    public GridDeploymentInfoBean(GridDeploymentInfo dep) {
        clsLdrId = dep.classLoaderId();
        depMode = dep.deployMode();
        userVer = dep.userVersion();
        locDepOwner = dep.localDeploymentOwner();
        participants = dep.participants();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /** {@inheritDoc} */
    @Override public DeploymentMode deployMode() {
        return depMode;
    }

    /** {@inheritDoc} */
    @Override public String userVersion() {
        return userVer;
    }

    /** {@inheritDoc} */
    @Override public long sequenceNumber() {
        return clsLdrId.localId();
    }

    /** {@inheritDoc} */
    @Override public boolean localDeploymentOwner() {
        return locDepOwner;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, IgniteUuid> participants() {
        return participants;
    }

    /**
     * Sets local deployment ownership flag.
     *
     * @param locDepOwner Local deployment ownership flag.
     */
    public void localDeploymentOwner(boolean locDepOwner) {
        this.locDepOwner = locDepOwner;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return clsLdrId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o == this || o instanceof GridDeploymentInfoBean &&
            clsLdrId.equals(((GridDeploymentInfoBean)o).clsLdrId);
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
                if (!writer.writeIgniteUuid("clsLdrId", clsLdrId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte("depMode", depMode != null ? (byte)depMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("locDepOwner", locDepOwner))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap("participants", participants, MessageCollectionItemType.UUID, MessageCollectionItemType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 4:
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
                clsLdrId = reader.readIgniteUuid("clsLdrId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                byte depModeOrd;

                depModeOrd = reader.readByte("depMode");

                if (!reader.isLastRead())
                    return false;

                depMode = DeploymentMode.fromOrdinal(depModeOrd);

                reader.incrementState();

            case 2:
                locDepOwner = reader.readBoolean("locDepOwner");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                participants = reader.readMap("participants", MessageCollectionItemType.UUID, MessageCollectionItemType.IGNITE_UUID, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                userVer = reader.readString("userVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDeploymentInfoBean.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, clsLdrId);
        U.writeEnum(out, depMode);
        U.writeString(out, userVer);
        out.writeBoolean(locDepOwner);
        U.writeMap(out, participants);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        clsLdrId = U.readGridUuid(in);
        depMode = DeploymentMode.fromOrdinal(in.readByte());
        userVer = U.readString(in);
        locDepOwner = in.readBoolean();
        participants = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentInfoBean.class, this);
    }
}