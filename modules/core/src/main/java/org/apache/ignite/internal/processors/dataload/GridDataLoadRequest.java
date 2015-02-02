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
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class GridDataLoadRequest extends GridTcpCommunicationMessageAdapter {
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
    private IgniteDeploymentMode depMode;

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
        IgniteDeploymentMode depMode,
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
    public IgniteDeploymentMode deploymentMode() {
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
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putString("cacheName", cacheName))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putGridUuid("clsLdrId", clsLdrId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putByteArray("colBytes", colBytes))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putEnum("depMode", depMode))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putBoolean("forceLocDep", forceLocDep))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putBoolean("ignoreDepOwnership", ignoreDepOwnership))
                    return false;

                commState.idx++;

            case 6:
                if (ldrParticipants != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, ldrParticipants.size()))
                            return false;

                        commState.it = ldrParticipants.entrySet().iterator();
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

            case 7:
                if (!commState.putLong("reqId", reqId))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putByteArray("resTopicBytes", resTopicBytes))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putString("sampleClsName", sampleClsName))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putBoolean("skipStore", skipStore))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putByteArray("updaterBytes", updaterBytes))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putString("userVer", userVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                cacheName = commState.getString("cacheName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                clsLdrId = commState.getGridUuid("clsLdrId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 2:
                colBytes = commState.getByteArray("colBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 3:
                byte depMode0 = commState.getByte("depMode");

                if (!commState.lastRead())
                    return false;

                depMode = IgniteDeploymentMode.fromOrdinal(depMode0);

                commState.idx++;

            case 4:
                forceLocDep = commState.getBoolean("forceLocDep");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 5:
                ignoreDepOwnership = commState.getBoolean("ignoreDepOwnership");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 6:
                if (commState.readSize == -1) {
                    commState.readSize = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                }

                if (commState.readSize >= 0) {
                    if (ldrParticipants == null)
                        ldrParticipants = new HashMap<>(commState.readSize, 1.0f);

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

                        ldrParticipants.put((UUID)commState.cur, _val);

                        commState.keyDone = false;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;
                commState.cur = null;

                commState.idx++;

            case 7:
                reqId = commState.getLong("reqId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 8:
                resTopicBytes = commState.getByteArray("resTopicBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 9:
                sampleClsName = commState.getString("sampleClsName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 10:
                skipStore = commState.getBoolean("skipStore");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 11:
                updaterBytes = commState.getByteArray("updaterBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 12:
                userVer = commState.getString("userVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 62;
    }

    /** {@inheritDoc} */
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDataLoadRequest _clone = new GridDataLoadRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
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
