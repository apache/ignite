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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Job execution request.
 */
@SuppressWarnings({"AssignmentOrReturnOfFieldWithMutableType", "NullableProblems"})
public class GridJobExecuteRequest implements ExecutorAwareMessage, DeferredUnmarshalMessage {
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
    @Marshalled("jobBytes")
    ComputeJob job;

    /** */
    @Order(3)
    long startTaskTime;

    /** */
    @Order(4)
    long timeout;

    /** */
    @Order(5)
    String taskName;

    /** Deployment of the task classes. */
    @Order(6)
    GridDeploymentInfoMessage depInfo;

    /** */
    @Order(7)
    String taskClsName;

    /** */
    @GridToStringExclude
    @Order(8)
    byte[] sesAttrsBytes;

    /** */
    @GridToStringExclude
    @Marshalled("sesAttrsBytes")
    Map<Object, Object> sesAttrs;

    /** */
    @GridToStringExclude
    @Order(9)
    byte[] jobAttrsBytes;

    /** */
    @GridToStringExclude
    @Marshalled("jobAttrsBytes")
    Map<? extends Serializable, ? extends Serializable> jobAttrs;

    /** Checkpoint SPI name. */
    @Order(10)
    String cpSpi;

    /** */
    @Order(11)
    @Nullable IgniteUuid[] siblingJobsIds;

    /** Transient since needs to hold local creation time. */
    private final long createTime = U.currentTimeMillis();

    /** */
    @Order(12)
    boolean forceLocDep;

    /** */
    @Order(13)
    boolean sesFullSup;

    /** */
    @Order(14)
    boolean internal;

    /** */
    @Order(15)
    Collection<UUID> top;

    /** */
    @Marshalled("topPredBytes")
    IgnitePredicate<ClusterNode> topPred;

    /** */
    @Order(16)
    byte[] topPredBytes;

    /** */
    @Order(17)
    int[] cacheIds;

    /** */
    @Order(18)
    int part;

    /** */
    @Order(19)
    AffinityTopologyVersion topVer;

    /** */
    @Order(20)
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
     * @param depInfo Deployment of the task classes.
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
     * @param dynamicSiblings {@code True} if siblings are dynamic.
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
            GridDeploymentInfo depInfo,
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
            boolean dynamicSiblings,
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
        assert depInfo != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.taskName = taskName;
        this.depInfo = new GridDeploymentInfoMessage(depInfo);
        this.taskClsName = taskClsName;
        this.job = job;
        this.startTaskTime = startTaskTime;
        this.timeout = timeout;
        this.top = top;
        this.topVer = topVer;
        this.topPred = topPred;
        this.sesAttrs = sesAttrs;
        this.jobAttrs = jobAttrs;
        this.forceLocDep = forceLocDep;
        this.sesFullSup = sesFullSup;
        this.internal = internal;
        this.cacheIds = cacheIds;
        this.part = part;
        this.topVer = topVer;
        this.execName = execName;

        this.cpSpi = cpSpi == null || cpSpi.isEmpty() ? null : cpSpi;

        if (!dynamicSiblings && !F.isEmpty(siblings))
            siblingJobsIds = siblings.stream().map(ComputeJobSibling::getJobId).toArray(IgniteUuid[]::new);
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

    /** @return Deployment of the task classes. */
    public GridDeploymentInfo deploymentInfo() {
        return depInfo;
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
     * @return Siblings jobs ids.
     */
    public @Nullable IgniteUuid[] siblingJobsIds() {
        return siblingJobsIds;
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
    @Override public String toString() {
        return S.toString(GridJobExecuteRequest.class, this);
    }

}
