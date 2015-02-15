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

package org.apache.ignite.internal.processors.streamer;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class GridStreamerExecutionRequest extends MessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Force local deployment flag. */
    private boolean forceLocDep;

    /** Serialized batch in case if P2P class loading is enabled. */
    @GridToStringExclude
    private byte[] batchBytes;

    /** Deployment mode. */
    private DeploymentMode depMode;

    /** Deployment sample class name. */
    private String sampleClsName;

    /** Deployment user version. */
    private String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = IgniteUuid.class)
    private Map<UUID, IgniteUuid> ldrParticipants;

    /** Class loader ID. */
    private IgniteUuid clsLdrId;

    /**
     *
     */
    public GridStreamerExecutionRequest() {
        // No-op.
    }

    /**
     * @param forceLocDep Force local deployment flag.
     * @param batchBytes Batch serialized bytes.
     * @param depMode Deployment mode.
     * @param sampleClsName Sample class name.
     * @param userVer User version.
     * @param ldrParticipants Loader participants.
     * @param clsLdrId Class loader ID.
     */
    public GridStreamerExecutionRequest(
        boolean forceLocDep,
        byte[] batchBytes,
        @Nullable DeploymentMode depMode,
        @Nullable String sampleClsName,
        @Nullable String userVer,
        @Nullable Map<UUID, IgniteUuid> ldrParticipants,
        @Nullable IgniteUuid clsLdrId
    ) {
        assert batchBytes != null;

        this.forceLocDep = forceLocDep;
        this.batchBytes = batchBytes;
        this.depMode = depMode;
        this.sampleClsName = sampleClsName;
        this.userVer = userVer;
        this.ldrParticipants = ldrParticipants;
        this.clsLdrId = clsLdrId;
    }

    /**
     * @return Force local deployment flag.
     */
    public boolean forceLocalDeployment() {
        return forceLocDep;
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Deployment sample class name.
     */
    public String sampleClassName() {
        return sampleClsName;
    }

    /**
     * @return Deployment user version.
     */
    public String userVersion() {
        return userVer;
    }

    /**
     * @return Node class loader participants.
     */
    public Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParticipants;
    }

    /**
     * @return Class loader ID.
     */
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Serialized batch in case if P2P class loading is enabled.
     */
    public byte[] batchBytes() {
        return batchBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridStreamerExecutionRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeField("batchBytes", batchBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeField("clsLdrId", clsLdrId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeField("depMode", depMode != null ? (byte)depMode.ordinal() : -1, MessageFieldType.BYTE))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeField("forceLocDep", forceLocDep, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMapField("ldrParticipants", ldrParticipants, MessageFieldType.UUID, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeField("sampleClsName", sampleClsName, MessageFieldType.STRING))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeField("userVer", userVer, MessageFieldType.STRING))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (readState) {
            case 0:
                batchBytes = reader.readField("batchBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                clsLdrId = reader.readField("clsLdrId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                byte depModeOrd;

                depModeOrd = reader.readField("depMode", MessageFieldType.BYTE);

                if (!reader.isLastRead())
                    return false;

                depMode = DeploymentMode.fromOrdinal(depModeOrd);

                readState++;

            case 3:
                forceLocDep = reader.readField("forceLocDep", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                ldrParticipants = reader.readMapField("ldrParticipants", MessageFieldType.UUID, MessageFieldType.IGNITE_UUID, false);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                sampleClsName = reader.readField("sampleClsName", MessageFieldType.STRING);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                userVer = reader.readField("userVer", MessageFieldType.STRING);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 80;
    }
}
