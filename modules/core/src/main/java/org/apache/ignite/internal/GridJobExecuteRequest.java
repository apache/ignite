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

package org.apache.ignite.internal;

import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Job execution request.
 */
public class GridJobExecuteRequest extends MessageAdapter implements GridTaskMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Subject ID. */
    private UUID subjId;

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
    private long createTime = U.currentTimeMillis();

    /** */
    private IgniteUuid clsLdrId;

    /** */
    private DeploymentMode depMode;

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
     * @param subjId Subject ID.
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
            DeploymentMode depMode,
            boolean dynamicSiblings,
            Map<UUID, IgniteUuid> ldrParticipants,
            boolean forceLocDep,
            boolean sesFullSup,
            boolean internal,
            UUID subjId) {
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
        this.subjId = subjId;

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
        return createTime;
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
    public DeploymentMode getDeploymentMode() {
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
        return subjId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridJobExecuteRequest _clone = new GridJobExecuteRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridJobExecuteRequest _clone = (GridJobExecuteRequest)_msg;

        _clone.subjId = subjId;
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
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putGridUuid("clsLdrId", clsLdrId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putString("cpSpi", cpSpi))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putEnum("depMode", depMode))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putBoolean("dynamicSiblings", dynamicSiblings))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putBoolean("forceLocDep", forceLocDep))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putBoolean("internal", internal))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putByteArray("jobAttrsBytes", jobAttrsBytes))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putByteArray("jobBytes", jobBytes))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putGridUuid("jobId", jobId))
                    return false;

                commState.idx++;

            case 9:
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

            case 10:
                if (!commState.putByteArray("sesAttrsBytes", sesAttrsBytes))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putBoolean("sesFullSup", sesFullSup))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putGridUuid("sesId", sesId))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putByteArray("siblingsBytes", siblingsBytes))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putLong("startTaskTime", startTaskTime))
                    return false;

                commState.idx++;

            case 15:
                if (!commState.putUuid("subjId", subjId))
                    return false;

                commState.idx++;

            case 16:
                if (!commState.putString("taskClsName", taskClsName))
                    return false;

                commState.idx++;

            case 17:
                if (!commState.putString("taskName", taskName))
                    return false;

                commState.idx++;

            case 18:
                if (!commState.putLong("timeout", timeout))
                    return false;

                commState.idx++;

            case 19:
                if (top != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, top.size()))
                            return false;

                        commState.it = top.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putUuid(null, (UUID)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 20:
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
                clsLdrId = commState.getGridUuid("clsLdrId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                cpSpi = commState.getString("cpSpi");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 2:
                byte depMode0 = commState.getByte("depMode");

                if (!commState.lastRead())
                    return false;

                depMode = DeploymentMode.fromOrdinal(depMode0);

                commState.idx++;

            case 3:
                dynamicSiblings = commState.getBoolean("dynamicSiblings");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 4:
                forceLocDep = commState.getBoolean("forceLocDep");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 5:
                internal = commState.getBoolean("internal");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 6:
                jobAttrsBytes = commState.getByteArray("jobAttrsBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 7:
                jobBytes = commState.getByteArray("jobBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 8:
                jobId = commState.getGridUuid("jobId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 9:
                if (commState.readSize == -1) {
                    int _val = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                    commState.readSize = _val;
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

            case 10:
                sesAttrsBytes = commState.getByteArray("sesAttrsBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 11:
                sesFullSup = commState.getBoolean("sesFullSup");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 12:
                sesId = commState.getGridUuid("sesId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 13:
                siblingsBytes = commState.getByteArray("siblingsBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 14:
                startTaskTime = commState.getLong("startTaskTime");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 15:
                subjId = commState.getUuid("subjId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 16:
                taskClsName = commState.getString("taskClsName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 17:
                taskName = commState.getString("taskName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 18:
                timeout = commState.getLong("timeout");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 19:
                if (commState.readSize == -1) {
                    int _val = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                    commState.readSize = _val;
                }

                if (commState.readSize >= 0) {
                    if (top == null)
                        top = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        UUID _val = commState.getUuid(null);

                        if (!commState.lastRead())
                            return false;

                        top.add((UUID)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 20:
                userVer = commState.getString("userVer");

                if (!commState.lastRead())
                    return false;

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
