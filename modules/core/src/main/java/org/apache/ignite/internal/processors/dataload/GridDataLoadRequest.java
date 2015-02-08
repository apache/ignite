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

package org.apache.ignite.internal.processors.dataload;

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
public class GridDataLoadRequest extends MessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long reqId;

    /** */
    private byte[] resTopicBytes;

    /** Cache name. */
    private String cacheName;

    /** */
    private byte[] updaterBytes;

    /** Entries to put. */
    private byte[] colBytes;

    /** {@code True} to ignore deployment ownership. */
    private boolean ignoreDepOwnership;

    /** */
    private boolean skipStore;

    /** */
    private DeploymentMode depMode;

    /** */
    private String sampleClsName;

    /** */
    private String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = IgniteUuid.class)
    private Map<UUID, IgniteUuid> ldrParticipants;

    /** */
    private IgniteUuid clsLdrId;

    /** */
    private boolean forceLocDep;

    /**
     * {@code Externalizable} support.
     */
    public GridDataLoadRequest() {
        // No-op.
    }

    /**
     * @param reqId Request ID.
     * @param resTopicBytes Response topic.
     * @param cacheName Cache name.
     * @param updaterBytes Cache updater.
     * @param colBytes Collection bytes.
     * @param ignoreDepOwnership Ignore ownership.
     * @param skipStore Skip store flag.
     * @param depMode Deployment mode.
     * @param sampleClsName Sample class name.
     * @param userVer User version.
     * @param ldrParticipants Loader participants.
     * @param clsLdrId Class loader ID.
     * @param forceLocDep Force local deployment.
     */
    public GridDataLoadRequest(long reqId,
        byte[] resTopicBytes,
        @Nullable String cacheName,
        byte[] updaterBytes,
        byte[] colBytes,
        boolean ignoreDepOwnership,
        boolean skipStore,
        DeploymentMode depMode,
        String sampleClsName,
        String userVer,
        Map<UUID, IgniteUuid> ldrParticipants,
        IgniteUuid clsLdrId,
        boolean forceLocDep) {
        this.reqId = reqId;
        this.resTopicBytes = resTopicBytes;
        this.cacheName = cacheName;
        this.updaterBytes = updaterBytes;
        this.colBytes = colBytes;
        this.ignoreDepOwnership = ignoreDepOwnership;
        this.skipStore = skipStore;
        this.depMode = depMode;
        this.sampleClsName = sampleClsName;
        this.userVer = userVer;
        this.ldrParticipants = ldrParticipants;
        this.clsLdrId = clsLdrId;
        this.forceLocDep = forceLocDep;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Response topic.
     */
    public byte[] responseTopicBytes() {
        return resTopicBytes;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Updater.
     */
    public byte[] updaterBytes() {
        return updaterBytes;
    }

    /**
     * @return Collection bytes.
     */
    public byte[] collectionBytes() {
        return colBytes;
    }

    /**
     * @return {@code True} to ignore ownership.
     */
    public boolean ignoreDeploymentOwnership() {
        return ignoreDepOwnership;
    }

    /**
     * @return Skip store flag.
     */
    public boolean skipStore() {
        return skipStore;
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Sample class name.
     */
    public String sampleClassName() {
        return sampleClsName;
    }

    /**
     * @return User version.
     */
    public String userVersion() {
        return userVer;
    }

    /**
     * @return Participants.
     */
    public Map<UUID, IgniteUuid> participants() {
        return ldrParticipants;
    }

    /**
     * @return Class loader ID.
     */
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return {@code True} to force local deployment.
     */
    public boolean forceLocalDeployment() {
        return forceLocDep;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDataLoadRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 0:
                if (!writer.writeString("cacheName", cacheName))
                    return false;

                state++;

            case 1:
                if (!writer.writeIgniteUuid("clsLdrId", clsLdrId))
                    return false;

                state++;

            case 2:
                if (!writer.writeByteArray("colBytes", colBytes))
                    return false;

                state++;

            case 3:
                if (!writer.writeEnum("depMode", depMode))
                    return false;

                state++;

            case 4:
                if (!writer.writeBoolean("forceLocDep", forceLocDep))
                    return false;

                state++;

            case 5:
                if (!writer.writeBoolean("ignoreDepOwnership", ignoreDepOwnership))
                    return false;

                state++;

            case 6:
                if (!writer.writeMap("ldrParticipants", ldrParticipants, UUID.class, IgniteUuid.class))
                    return false;

                state++;

            case 7:
                if (!writer.writeLong("reqId", reqId))
                    return false;

                state++;

            case 8:
                if (!writer.writeByteArray("resTopicBytes", resTopicBytes))
                    return false;

                state++;

            case 9:
                if (!writer.writeString("sampleClsName", sampleClsName))
                    return false;

                state++;

            case 10:
                if (!writer.writeBoolean("skipStore", skipStore))
                    return false;

                state++;

            case 11:
                if (!writer.writeByteArray("updaterBytes", updaterBytes))
                    return false;

                state++;

            case 12:
                if (!writer.writeString("userVer", userVer))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (state) {
            case 0:
                cacheName = reader.readString("cacheName");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 1:
                clsLdrId = reader.readIgniteUuid("clsLdrId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 2:
                colBytes = reader.readByteArray("colBytes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 3:
                depMode = reader.readEnum("depMode", DeploymentMode.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 4:
                forceLocDep = reader.readBoolean("forceLocDep");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 5:
                ignoreDepOwnership = reader.readBoolean("ignoreDepOwnership");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 6:
                ldrParticipants = reader.readMap("ldrParticipants", UUID.class, IgniteUuid.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 7:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 8:
                resTopicBytes = reader.readByteArray("resTopicBytes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 9:
                sampleClsName = reader.readString("sampleClsName");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 10:
                skipStore = reader.readBoolean("skipStore");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 11:
                updaterBytes = reader.readByteArray("updaterBytes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 12:
                userVer = reader.readString("userVer");

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 62;
    }

    /** {@inheritDoc} */
    @Override public MessageAdapter clone() {
        GridDataLoadRequest _clone = new GridDataLoadRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridDataLoadRequest _clone = (GridDataLoadRequest)_msg;

        _clone.reqId = reqId;
        _clone.resTopicBytes = resTopicBytes;
        _clone.cacheName = cacheName;
        _clone.updaterBytes = updaterBytes;
        _clone.colBytes = colBytes;
        _clone.ignoreDepOwnership = ignoreDepOwnership;
        _clone.skipStore = skipStore;
        _clone.depMode = depMode;
        _clone.sampleClsName = sampleClsName;
        _clone.userVer = userVer;
        _clone.ldrParticipants = ldrParticipants;
        _clone.clsLdrId = clsLdrId;
        _clone.forceLocDep = forceLocDep;
    }
}
