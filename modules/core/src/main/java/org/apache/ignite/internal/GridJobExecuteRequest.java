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

import java.io.Externalizable;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Job execution request.
 */
public class GridJobExecuteRequest implements Message {
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
    
    /**
     * @return Task session ID.
     */
    public IgniteUuid getSessionId() {
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
                if (!writer.writeString("cpSpi", cpSpi))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByte("depMode", depMode != null ? (byte)depMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeBoolean("dynamicSiblings", dynamicSiblings))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeBoolean("forceLocDep", forceLocDep))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeBoolean("internal", internal))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByteArray("jobAttrsBytes", jobAttrsBytes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByteArray("jobBytes", jobBytes))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeIgniteUuid("jobId", jobId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap("ldrParticipants", ldrParticipants, MessageCollectionItemType.UUID, MessageCollectionItemType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeByteArray("sesAttrsBytes", sesAttrsBytes))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("sesFullSup", sesFullSup))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeIgniteUuid("sesId", sesId))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeByteArray("siblingsBytes", siblingsBytes))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeLong("startTaskTime", startTaskTime))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeString("taskClsName", taskClsName))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeString("taskName", taskName))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeCollection("top", top, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 20:
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
                cpSpi = reader.readString("cpSpi");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                byte depModeOrd;

                depModeOrd = reader.readByte("depMode");

                if (!reader.isLastRead())
                    return false;

                depMode = DeploymentMode.fromOrdinal(depModeOrd);

                reader.incrementState();

            case 3:
                dynamicSiblings = reader.readBoolean("dynamicSiblings");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                forceLocDep = reader.readBoolean("forceLocDep");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                internal = reader.readBoolean("internal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                jobAttrsBytes = reader.readByteArray("jobAttrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                jobBytes = reader.readByteArray("jobBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                jobId = reader.readIgniteUuid("jobId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                ldrParticipants = reader.readMap("ldrParticipants", MessageCollectionItemType.UUID, MessageCollectionItemType.IGNITE_UUID, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                sesAttrsBytes = reader.readByteArray("sesAttrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                sesFullSup = reader.readBoolean("sesFullSup");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                sesId = reader.readIgniteUuid("sesId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                siblingsBytes = reader.readByteArray("siblingsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                startTaskTime = reader.readLong("startTaskTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                taskClsName = reader.readString("taskClsName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                taskName = reader.readString("taskName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                top = reader.readCollection("top", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                userVer = reader.readString("userVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridJobExecuteRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 21;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobExecuteRequest.class, this);
    }
}