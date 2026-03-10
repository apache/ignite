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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Job execution request.
 */
@SuppressWarnings({"AssignmentOrReturnOfFieldWithMutableType", "NullableProblems"})
public class GridJobExecuteRequest implements ExecutorAwareMessage {
    /** */
    @Order(0)
    IgniteUuid sesId;

    /** */
    @Order(1)
    IgniteUuid jobId;

    /** */
    @GridToStringExclude
    @Order(2)
    byte[] jobBytes;

    /** */
    @GridToStringExclude
    private ComputeJob job;

    /** */
    @Order(3)
    long startTaskTime;

    /** */
    @Order(4)
    long timeout;

    /** */
    @Order(5)
    String taskName;

    /** */
    @Order(6)
    String userVer;

    /** */
    @Order(7)
    String taskClsName;

    /** Node class loader participants. */
    @GridToStringInclude
    @Order(8)
    Map<UUID, IgniteUuid> ldrParticipants;

    /** */
    @GridToStringExclude
    @Order(9)
    byte[] sesAttrsBytes;

    /** */
    @GridToStringExclude
    private Map<Object, Object> sesAttrs;

    /** */
    @GridToStringExclude
    @Order(10)
    byte[] jobAttrsBytes;

    /** */
    @GridToStringExclude
    private Map<? extends Serializable, ? extends Serializable> jobAttrs;

    /** Checkpoint SPI name. */
    @Order(11)
    String cpSpi;

    /** */
    private Collection<ComputeJobSibling> siblings;

    /** */
    @Order(12)
    byte[] siblingsBytes;

    /** Transient since needs to hold local creation time. */
    private final long createTime = U.currentTimeMillis();

    /** */
    @Order(13)
    IgniteUuid clsLdrId;

    /** */
    @Order(14)
    DeploymentMode depMode;

    /** */
    @Order(15)
    boolean dynamicSiblings;

    /** */
    @Order(16)
    boolean forceLocDep;

    /** */
    @Order(17)
    boolean sesFullSup;

    /** */
    @Order(18)
    boolean internal;

    /** */
    @Order(19)
    Collection<UUID> top;

    /** */
    private IgnitePredicate<ClusterNode> topPred;

    /** */
    @Order(20)
    byte[] topPredBytes;

    /** */
    @Order(21)
    int[] cacheIds;

    /** */
    @Order(22)
    int part;

    /** */
    @Order(23)
    AffinityTopologyVersion topVer;

    /** */
    @Order(24)
    String execName;

    /**
     * Default constructor.
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
     * @param job Job.
     * @param startTaskTime Task execution start time.
     * @param timeout Task execution timeout.
     * @param top Topology.
     * @param topPred Topology predicate.
     * @param siblings Collection of split siblings.
     * @param sesAttrs Session attributes.
     * @param jobAttrs Job attributes.
     * @param cpSpi Collision SPI.
     * @param clsLdrId Task local class loader id.
     * @param depMode Task deployment mode.
     * @param dynamicSiblings {@code True} if siblings are dynamic.
     * @param ldrParticipants Other node class loader IDs that can also load classes.
     * @param forceLocDep {@code True} If remote node should ignore deployment settings.
     * @param sesFullSup {@code True} if session attributes are disabled.
     * @param internal {@code True} if internal job.
     * @param cacheIds Caches' identifiers to reserve partition.
     * @param part Partition to lock.
     * @param topVer Affinity topology version of job mapping.
     * @param execName The name of the custom named executor.
     */
    public GridJobExecuteRequest(
            IgniteUuid sesId,
            IgniteUuid jobId,
            String taskName,
            String userVer,
            String taskClsName,
            ComputeJob job,
            long startTaskTime,
            long timeout,
            @Nullable Collection<UUID> top,
            @Nullable IgnitePredicate<ClusterNode> topPred,
            Collection<ComputeJobSibling> siblings,
            Map<Object, Object> sesAttrs,
            Map<? extends Serializable, ? extends Serializable> jobAttrs,
            String cpSpi,
            IgniteUuid clsLdrId,
            DeploymentMode depMode,
            boolean dynamicSiblings,
            Map<UUID, IgniteUuid> ldrParticipants,
            boolean forceLocDep,
            boolean sesFullSup,
            boolean internal,
            @Nullable int[] cacheIds,
            int part,
            @Nullable AffinityTopologyVersion topVer,
            @Nullable String execName) {
        assert sesId != null;
        assert jobId != null;
        assert taskName != null;
        assert taskClsName != null;
        assert job != null;
        assert sesAttrs != null || !sesFullSup;
        assert jobAttrs != null;
        assert top != null || topPred != null;
        assert clsLdrId != null;
        assert userVer != null;
        assert depMode != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.taskName = taskName;
        this.userVer = userVer;
        this.taskClsName = taskClsName;
        this.job = job;
        this.startTaskTime = startTaskTime;
        this.timeout = timeout;
        this.top = top;
        this.topVer = topVer;
        this.topPred = topPred;
        this.siblings = siblings;
        this.sesAttrs = sesAttrs;
        this.jobAttrs = jobAttrs;
        this.clsLdrId = clsLdrId;
        this.depMode = depMode;
        this.dynamicSiblings = dynamicSiblings;
        this.ldrParticipants = ldrParticipants;
        this.forceLocDep = forceLocDep;
        this.sesFullSup = sesFullSup;
        this.internal = internal;
        this.cacheIds = cacheIds;
        this.part = part;
        this.topVer = topVer;
        this.execName = execName;

        this.cpSpi = cpSpi == null || cpSpi.isEmpty() ? null : cpSpi;
    }

    /**
     * @return Task session ID.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * @return Job session ID.
     */
    public IgniteUuid jobId() {
        return jobId;
    }

    /**
     * @return Task class name.
     */
    public String taskClassName() {
        return taskClsName;
    }

    /**
     * @return Task name.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @return Task version.
     */
    public String userVersion() {
        return userVer;
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
    public long startTaskTime() {
        return startTaskTime;
    }

    /**
     * @return Timeout.
     */
    public long timeout() {
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
     * @return Job siblings.
     */
    public Collection<ComputeJobSibling> getSiblings() {
        return siblings;
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
    public Map<? extends Serializable, ? extends Serializable> getJobAttributes() {
        return jobAttrs;
    }

    /**
     * @return Checkpoint SPI name.
     */
    public String checkpointSpi() {
        return cpSpi;
    }

    /**
     * @return Task local class loader id.
     */
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Node class loader participant map.
     */
    public Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParticipants;
    }

    /**
     * @return Returns {@code true} if deployment should always be used.
     */
    public boolean forceLocalDeployment() {
        return forceLocDep;
    }

    /**
     * @return Topology.
     */
    @Nullable public Collection<UUID> topology() {
        return top;
    }

    /**
     * @return Topology predicate.
     */
    public IgnitePredicate<ClusterNode> getTopologyPredicate() {
        return topPred;
    }

    /**
     * @return {@code True} if session attributes are enabled.
     */
    public boolean sessionFullSupport() {
        return sesFullSup;
    }

    /**
     * @return {@code True} if internal job.
     */
    public boolean internal() {
        return internal;
    }

    /**
     * @return Caches' identifiers to reserve specified partition for job execution.
     */
    public int[] cacheIds() {
        return cacheIds;
    }

    /**
     * @return Partition to lock for job execution.
     */
    public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public String executorName() {
        return execName;
    }

    /**
     * @return Affinity version which was used to map job
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobExecuteRequest.class, this);
    }

    /**
     * @param marsh Marshaller.
     */
    public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        jobBytes = U.marshal(marsh, job);
        topPredBytes = U.marshal(marsh, topPred);
        siblingsBytes = U.marshal(marsh, siblings);
        sesAttrsBytes = U.marshal(marsh, sesAttrs);
        jobAttrsBytes = U.marshal(marsh, jobAttrs);
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        assert top != null || topPredBytes != null;
        assert sesAttrsBytes != null || !sesFullSup;

        if (!dynamicSiblings && siblings == null)
            siblings = U.unmarshal(marsh, siblingsBytes, ldr);

        if (sesFullSup && sesAttrs == null)
            sesAttrs = U.unmarshal(marsh, sesAttrsBytes, ldr);

        if (topPred == null && topPredBytes != null)
            topPred = U.unmarshal(marsh, topPredBytes, ldr);

        if (jobAttrs == null)
            jobAttrs = U.unmarshal(marsh, jobAttrsBytes, ldr);

        if (job == null)
            job = U.unmarshal(marsh, jobBytes, ldr);

        // Are not required anymore.
        siblingsBytes = null;
        sesAttrsBytes = null;
        topPredBytes = null;
        jobAttrsBytes = null;
        jobBytes = null;
    }
}
