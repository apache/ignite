/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class GridStreamerExecutionRequest extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Force local deployment flag. */
    private boolean forceLocDep;

    /** Serialized batch in case if P2P class loading is enabled. */
    @GridToStringExclude
    private byte[] batchBytes;

    /** Deployment mode. */
    private IgniteDeploymentMode depMode;

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
        @Nullable IgniteDeploymentMode depMode,
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
    public IgniteDeploymentMode deploymentMode() {
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
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridStreamerExecutionRequest _clone = new GridStreamerExecutionRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridStreamerExecutionRequest _clone = (GridStreamerExecutionRequest)_msg;

        _clone.forceLocDep = forceLocDep;
        _clone.batchBytes = batchBytes;
        _clone.depMode = depMode;
        _clone.sampleClsName = sampleClsName;
        _clone.userVer = userVer;
        _clone.ldrParticipants = ldrParticipants;
        _clone.clsLdrId = clsLdrId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray(batchBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putGridUuid(clsLdrId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putEnum(depMode))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putBoolean(forceLocDep))
                    return false;

                commState.idx++;

            case 4:
                if (ldrParticipants != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(ldrParticipants.size()))
                            return false;

                        commState.it = ldrParticipants.entrySet().iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        Map.Entry<UUID, IgniteUuid> e = (Map.Entry<UUID, IgniteUuid>)commState.cur;

                        if (!commState.keyDone) {
                            if (!commState.putUuid(e.getKey()))
                                return false;

                            commState.keyDone = true;
                        }

                        if (!commState.putGridUuid(e.getValue()))
                            return false;

                        commState.keyDone = false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 5:
                if (!commState.putString(sampleClsName))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putString(userVer))
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
                byte[] batchBytes0 = commState.getByteArray();

                if (batchBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                batchBytes = batchBytes0;

                commState.idx++;

            case 1:
                IgniteUuid clsLdrId0 = commState.getGridUuid();

                if (clsLdrId0 == GRID_UUID_NOT_READ)
                    return false;

                clsLdrId = clsLdrId0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                byte depMode0 = commState.getByte();

                depMode = IgniteDeploymentMode.fromOrdinal(depMode0);

                commState.idx++;

            case 3:
                if (buf.remaining() < 1)
                    return false;

                forceLocDep = commState.getBoolean();

                commState.idx++;

            case 4:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (ldrParticipants == null)
                        ldrParticipants = U.newHashMap(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (!commState.keyDone) {
                            UUID _val = commState.getUuid();

                            if (_val == UUID_NOT_READ)
                                return false;

                            commState.cur = _val;
                            commState.keyDone = true;
                        }

                        IgniteUuid _val = commState.getGridUuid();

                        if (_val == GRID_UUID_NOT_READ)
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

            case 5:
                String sampleClsName0 = commState.getString();

                if (sampleClsName0 == STR_NOT_READ)
                    return false;

                sampleClsName = sampleClsName0;

                commState.idx++;

            case 6:
                String userVer0 = commState.getString();

                if (userVer0 == STR_NOT_READ)
                    return false;

                userVer = userVer0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 76;
    }
}
