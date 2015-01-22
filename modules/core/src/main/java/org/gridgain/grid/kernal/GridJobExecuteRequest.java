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

package org.gridgain.grid.kernal;

import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Job execution request.
 */
public class GridJobExecuteRequest extends GridTcpCommunicationMessageAdapter implements GridTaskMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid sesId;

    /** */
    private IgniteUuid jobId;

    /** */
    @GridToStringExclude
    private byte[] jobBytes;

    /** */
    @GridToStringExclude
    @GridDirectTransient
    private ComputeJob job;

    /** */
    private long startTaskTime;

    /** */
    private long timeout;

    /** */
    private String taskName;

    /** */
    private String userVer;

    /** */
    private String taskClsName;

    /** Node class loader participants. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = IgniteUuid.class)
    private Map<UUID, IgniteUuid> ldrParticipants;

    /** */
    @GridToStringExclude
    private byte[] sesAttrsBytes;

    /** */
    @GridToStringExclude
    @GridDirectTransient
    private Map<Object, Object> sesAttrs;

    /** */
    @GridToStringExclude
    private byte[] jobAttrsBytes;

    /** */
    @GridToStringExclude
    @GridDirectTransient
    private Map<? extends Serializable, ? extends Serializable> jobAttrs;

    /** Checkpoint SPI name. */
    private String cpSpi;

    /** */
    @GridDirectTransient
    private Collection<ComputeJobSibling> siblings;

    /** */
    private byte[] siblingsBytes;

    /** Transient since needs to hold local creation time. */
    @GridDirectTransient
    private long createTime0 = U.currentTimeMillis();

    /** @deprecated need to remove and use only {@link #createTime0}. */
    @Deprecated
    private long createTime = createTime0;

    /** */
    private IgniteUuid clsLdrId;

    /** */
    private IgniteDeploymentMode depMode;

    /** */
    private boolean dynamicSiblings;

    /** */
    private boolean forceLocDep;

    /** */
    private boolean sesFullSup;

    /** */
    private boolean internal;

    /** */
    @GridDirectCollection(UUID.class)
    private Collection<UUID> top;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     */
    public GridJobExecuteRequest() {
        // No-op.
    }

    /**
     * @param sesId Task session ID.
     * @param jobId Job ID.
     * @param taskName Task name.
     * @param userVer Code version.
     * @param taskClsName Fully qualified task name.
     * @param jobBytes Job serialized body.
     * @param job Job.
     * @param startTaskTime Task execution start time.
     * @param timeout Task execution timeout.
     * @param top Topology.
     * @param siblingsBytes Serialized collection of split siblings.
     * @param siblings Collection of split siblings.
     * @param sesAttrsBytes Map of session attributes.
     * @param sesAttrs Session attributes.
     * @param jobAttrsBytes Job context attributes.
     * @param jobAttrs Job attributes.
     * @param cpSpi Collision SPI.
     * @param clsLdrId Task local class loader id.
     * @param depMode Task deployment mode.
     * @param dynamicSiblings {@code True} if siblings are dynamic.
     * @param ldrParticipants Other node class loader IDs that can also load classes.
     * @param forceLocDep {@code True} If remote node should ignore deployment settings.
     * @param sesFullSup {@code True} if session attributes are disabled.
     * @param internal {@code True} if internal job.
     */
    public GridJobExecuteRequest(
        IgniteUuid sesId,
        IgniteUuid jobId,
        String taskName,
        String userVer,
        String taskClsName,
        byte[] jobBytes,
        ComputeJob job,
        long startTaskTime,
        long timeout,
        @Nullable Collection<UUID> top,
        byte[] siblingsBytes,
        Collection<ComputeJobSibling> siblings,
        byte[] sesAttrsBytes,
        Map<Object, Object> sesAttrs,
        byte[] jobAttrsBytes,
        Map<? extends Serializable, ? extends Serializable> jobAttrs,
        String cpSpi,
        IgniteUuid clsLdrId,
        IgniteDeploymentMode depMode,
        boolean dynamicSiblings,
        Map<UUID, IgniteUuid> ldrParticipants,
        boolean forceLocDep,
        boolean sesFullSup,
        boolean internal) {
        this.top = top;
        assert sesId != null;
        assert jobId != null;
        assert taskName != null;
        assert taskClsName != null;
        assert job != null || jobBytes != null;
        assert sesAttrs != null || sesAttrsBytes != null || !sesFullSup;
        assert jobAttrs != null || jobAttrsBytes != null;
        assert clsLdrId != null;
        assert userVer != null;
        assert depMode != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.taskName = taskName;
        this.userVer = userVer;
        this.taskClsName = taskClsName;
        this.jobBytes = jobBytes;
        this.job = job;
        this.startTaskTime = startTaskTime;
        this.timeout = timeout;
        this.top = top;
        this.siblingsBytes = siblingsBytes;
        this.siblings = siblings;
        this.sesAttrsBytes = sesAttrsBytes;
        this.sesAttrs = sesAttrs;
        this.jobAttrsBytes = jobAttrsBytes;
        this.jobAttrs = jobAttrs;
        this.clsLdrId = clsLdrId;
        this.depMode = depMode;
        this.dynamicSiblings = dynamicSiblings;
        this.ldrParticipants = ldrParticipants;
        this.forceLocDep = forceLocDep;
        this.sesFullSup = sesFullSup;
        this.internal = internal;

        this.cpSpi = cpSpi == null || cpSpi.isEmpty() ? null : cpSpi;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getSessionId() {
        return sesId;
    }

    /**
     * @return Job session ID.
     */
    public IgniteUuid getJobId() {
        return jobId;
    }

    /**
     * @return Task version.
     */
    public String getTaskClassName() {
        return taskClsName;
    }

    /**
     * @return Task name.
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * @return Task version.
     */
    public String getUserVersion() {
        return userVer;
    }

    /**
     * @return Serialized job bytes.
     */
    public byte[] getJobBytes() {
        return jobBytes;
    }

    /**
     * @return Grid job.
     */
    public ComputeJob getJob() {
        return job;
    }

    /**
     * @return Task start time.
     */
    public long getStartTaskTime() {
        return startTaskTime;
    }

    /**
     * @return Timeout.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Gets this instance creation time.
     *
     * @return This instance creation time.
     */
    public long getCreateTime() {
        return createTime0;
    }

    /**
     * @return Serialized collection of split siblings.
     */
    public byte[] getSiblingsBytes() {
        return siblingsBytes;
    }

    /**
     * @return Job siblings.
     */
    public Collection<ComputeJobSibling> getSiblings() {
        return siblings;
    }

    /**
     * @return Session attributes.
     */
    public byte[] getSessionAttributesBytes() {
        return sesAttrsBytes;
    }

    /**
     * @return Session attributes.
     */
    public Map<Object, Object> getSessionAttributes() {
        return sesAttrs;
    }

    /**
     * @return Job attributes.
     */
    public byte[] getJobAttributesBytes() {
        return jobAttrsBytes;
    }

    /**
     * @return Job attributes.
     */
    public Map<? extends Serializable, ? extends Serializable> getJobAttributes() {
        return jobAttrs;
    }

    /**
     * @return Checkpoint SPI name.
     */
    public String getCheckpointSpi() {
        return cpSpi;
    }

    /**
     * @return Task local class loader id.
     */
    public IgniteUuid getClassLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    public IgniteDeploymentMode getDeploymentMode() {
        return depMode;
    }

    /**
     * Returns true if siblings list is dynamic, i.e. task is continuous.
     *
     * @return True if siblings list is dynamic.
     */
    public boolean isDynamicSiblings() {
        return dynamicSiblings;
    }

    /**
     * @return Node class loader participant map.
     */
    public Map<UUID, IgniteUuid> getLoaderParticipants() {
        return ldrParticipants;
    }

    /**
     * @return Returns {@code true} if deployment should always be used.
     */
    public boolean isForceLocalDeployment() {
        return forceLocDep;
    }

    /**
     * @return Topology.
     */
    @Nullable public Collection<UUID> topology() {
        return top;
    }
    /**
     * @return {@code True} if session attributes are enabled.
     */
    public boolean isSessionFullSupport() {
        return sesFullSup;
    }

    /**
     * @return {@code True} if internal job.
     */
    public boolean isInternal() {
        return internal;
    }

    /**
     * @return Subject ID.
     */
    public UUID getSubjectId() {
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridJobExecuteRequest _clone = new GridJobExecuteRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridJobExecuteRequest _clone = (GridJobExecuteRequest)_msg;

        _clone.sesId = sesId;
        _clone.jobId = jobId;
        _clone.jobBytes = jobBytes;
        _clone.job = job;
        _clone.startTaskTime = startTaskTime;
        _clone.timeout = timeout;
        _clone.taskName = taskName;
        _clone.userVer = userVer;
        _clone.taskClsName = taskClsName;
        _clone.ldrParticipants = ldrParticipants;
        _clone.sesAttrsBytes = sesAttrsBytes;
        _clone.sesAttrs = sesAttrs;
        _clone.jobAttrsBytes = jobAttrsBytes;
        _clone.jobAttrs = jobAttrs;
        _clone.cpSpi = cpSpi;
        _clone.siblings = siblings;
        _clone.siblingsBytes = siblingsBytes;
        _clone.createTime = createTime;
        _clone.createTime0 = createTime0;
        _clone.clsLdrId = clsLdrId;
        _clone.depMode = depMode;
        _clone.dynamicSiblings = dynamicSiblings;
        _clone.forceLocDep = forceLocDep;
        _clone.sesFullSup = sesFullSup;
        _clone.internal = internal;
        _clone.top = top;
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
                if (!commState.putGridUuid(clsLdrId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putString(cpSpi))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putLong(createTime))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putEnum(depMode))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putBoolean(dynamicSiblings))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putBoolean(forceLocDep))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putBoolean(internal))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putByteArray(jobAttrsBytes))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putByteArray(jobBytes))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putGridUuid(jobId))
                    return false;

                commState.idx++;

            case 10:
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

            case 11:
                if (!commState.putByteArray(sesAttrsBytes))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putBoolean(sesFullSup))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putGridUuid(sesId))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putByteArray(siblingsBytes))
                    return false;

                commState.idx++;

            case 15:
                if (!commState.putLong(startTaskTime))
                    return false;

                commState.idx++;

            case 16:
                if (!commState.putString(taskClsName))
                    return false;

                commState.idx++;

            case 17:
                if (!commState.putString(taskName))
                    return false;

                commState.idx++;

            case 18:
                if (!commState.putLong(timeout))
                    return false;

                commState.idx++;

            case 19:
                if (top != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(top.size()))
                            return false;

                        commState.it = top.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putUuid((UUID)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 20:
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
                IgniteUuid clsLdrId0 = commState.getGridUuid();

                if (clsLdrId0 == GRID_UUID_NOT_READ)
                    return false;

                clsLdrId = clsLdrId0;

                commState.idx++;

            case 1:
                String cpSpi0 = commState.getString();

                if (cpSpi0 == STR_NOT_READ)
                    return false;

                cpSpi = cpSpi0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 8)
                    return false;

                createTime = commState.getLong();

                commState.idx++;

            case 3:
                if (buf.remaining() < 1)
                    return false;

                byte depMode0 = commState.getByte();

                depMode = IgniteDeploymentMode.fromOrdinal(depMode0);

                commState.idx++;

            case 4:
                if (buf.remaining() < 1)
                    return false;

                dynamicSiblings = commState.getBoolean();

                commState.idx++;

            case 5:
                if (buf.remaining() < 1)
                    return false;

                forceLocDep = commState.getBoolean();

                commState.idx++;

            case 6:
                if (buf.remaining() < 1)
                    return false;

                internal = commState.getBoolean();

                commState.idx++;

            case 7:
                byte[] jobAttrsBytes0 = commState.getByteArray();

                if (jobAttrsBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                jobAttrsBytes = jobAttrsBytes0;

                commState.idx++;

            case 8:
                byte[] jobBytes0 = commState.getByteArray();

                if (jobBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                jobBytes = jobBytes0;

                commState.idx++;

            case 9:
                IgniteUuid jobId0 = commState.getGridUuid();

                if (jobId0 == GRID_UUID_NOT_READ)
                    return false;

                jobId = jobId0;

                commState.idx++;

            case 10:
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

            case 11:
                byte[] sesAttrsBytes0 = commState.getByteArray();

                if (sesAttrsBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                sesAttrsBytes = sesAttrsBytes0;

                commState.idx++;

            case 12:
                if (buf.remaining() < 1)
                    return false;

                sesFullSup = commState.getBoolean();

                commState.idx++;

            case 13:
                IgniteUuid sesId0 = commState.getGridUuid();

                if (sesId0 == GRID_UUID_NOT_READ)
                    return false;

                sesId = sesId0;

                commState.idx++;

            case 14:
                byte[] siblingsBytes0 = commState.getByteArray();

                if (siblingsBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                siblingsBytes = siblingsBytes0;

                commState.idx++;

            case 15:
                if (buf.remaining() < 8)
                    return false;

                startTaskTime = commState.getLong();

                commState.idx++;

            case 16:
                String taskClsName0 = commState.getString();

                if (taskClsName0 == STR_NOT_READ)
                    return false;

                taskClsName = taskClsName0;

                commState.idx++;

            case 17:
                String taskName0 = commState.getString();

                if (taskName0 == STR_NOT_READ)
                    return false;

                taskName = taskName0;

                commState.idx++;

            case 18:
                if (buf.remaining() < 8)
                    return false;

                timeout = commState.getLong();

                commState.idx++;

            case 19:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (top == null)
                        top = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        UUID _val = commState.getUuid();

                        if (_val == UUID_NOT_READ)
                            return false;

                        top.add((UUID)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 20:
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
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobExecuteRequest.class, this);
    }
}
