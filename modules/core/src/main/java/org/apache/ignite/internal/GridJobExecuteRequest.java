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
import org.apache.ignite.internal.managers.communication.DeploymentModeMessage;
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
    @Order(value = 0, method = "sessionId")
    private IgniteUuid sesId;

    /** */
    @Order(1)
    private IgniteUuid jobId;

    /** */
    @GridToStringExclude
    @Order(2)
    private byte[] jobBytes;

    /** */
    @GridToStringExclude
    private ComputeJob job;

    /** */
    @Order(3)
    private long startTaskTime;

    /** */
    @Order(4)
    private long timeout;

    /** */
    @Order(5)
    private String taskName;

    /** */
    @Order(value = 6, method = "userVersion")
    private String userVer;

    /** */
    @Order(value = 7, method = "taskClassName")
    private String taskClsName;

    /** Node class loader participants. */
    @GridToStringInclude
    @Order(value = 8, method = "loaderParticipants")
    private Map<UUID, IgniteUuid> ldrParticipants;

    /** */
    @GridToStringExclude
    @Order(value = 9, method = "sessionAttributesBytes")
    private byte[] sesAttrsBytes;

    /** */
    @GridToStringExclude
    private Map<Object, Object> sesAttrs;

    /** */
    @GridToStringExclude
    @Order(value = 10, method = "jobAttributesBytes")
    private byte[] jobAttrsBytes;

    /** */
    @GridToStringExclude
    private Map<? extends Serializable, ? extends Serializable> jobAttrs;

    /** Checkpoint SPI name. */
    @Order(value = 11, method = "checkpointSpi")
    private String cpSpi;

    /** */
    private Collection<ComputeJobSibling> siblings;

    /** */
    @Order(12)
    private byte[] siblingsBytes;

    /** Transient since needs to hold local creation time. */
    private final long createTime = U.currentTimeMillis();

    /** */
    @Order(value = 13, method = "classLoaderId")
    private IgniteUuid clsLdrId;

    /** */
    @Order(value = 14, method = "deploymentModeMessage")
    private DeploymentModeMessage depModeMsg;

    /** */
    @Order(15)
    private boolean dynamicSiblings;

    /** */
    @Order(value = 16, method = "forceLocalDeployment")
    private boolean forceLocDep;

    /** */
    @Order(value = 17, method = "sessionFullSupport")
    private boolean sesFullSup;

    /** */
    @Order(18)
    private boolean internal;

    /** */
    @Order(value = 19, method = "topology")
    private Collection<UUID> top;

    /** */
    private IgnitePredicate<ClusterNode> topPred;

    /** */
    @Order(value = 20, method = "topologyPredicateBytes")
    private byte[] topPredBytes;

    /** */
    @Order(21)
    private int[] cacheIds;

    /** */
    @Order(value = 22, method = "partition")
    private int part;

    /** */
    @Order(value = 23, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** */
    @Order(value = 24, method = "executorName")
    private String execName;

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
        depModeMsg = new DeploymentModeMessage(depMode);
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
     * @param sesId New task session ID.
     */
    public void sessionId(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /**
     * @return Job session ID.
     */
    public IgniteUuid jobId() {
        return jobId;
    }

    /**
     * @param jobId New job session ID.
     */
    public void jobId(IgniteUuid jobId) {
        this.jobId = jobId;
    }

    /**
     * @return Task class name.
     */
    public String taskClassName() {
        return taskClsName;
    }

    /**
     * @param taskClsName New task class name.
     */
    public void taskClassName(String taskClsName) {
        this.taskClsName = taskClsName;
    }

    /**
     * @return Task name.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @param taskName New task name.
     */
    public void taskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * @return Task version.
     */
    public String userVersion() {
        return userVer;
    }

    /**
     * @param userVer New task version.
     */
    public void userVersion(String userVer) {
        this.userVer = userVer;
    }

    /**
     * @return Serialized job bytes.
     */
    public byte[] jobBytes() {
        return jobBytes;
    }

    /**
     * @param jobBytes New serialized job bytes.
     */
    public void jobBytes(byte[] jobBytes) {
        this.jobBytes = jobBytes;
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
     * @param startTaskTime New task start time.
     */
    public void startTaskTime(long startTaskTime) {
        this.startTaskTime = startTaskTime;
    }

    /**
     * @return Timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout New timeout.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
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
    public byte[] siblingsBytes() {
        return siblingsBytes;
    }

    /**
     * @param siblingsBytes New serialized collection of split siblings.
     */
    public void siblingsBytes(byte[] siblingsBytes) {
        this.siblingsBytes = siblingsBytes;
    }

    /**
     * @return Job siblings.
     */
    public Collection<ComputeJobSibling> getSiblings() {
        return siblings;
    }

    /**
     * @return Serialized form of session attributes.
     */
    public byte[] sessionAttributesBytes() {
        return sesAttrsBytes;
    }

    /**
     * @param sesAttrsBytes New serialized form of session attributes.
     */
    public void sessionAttributesBytes(byte[] sesAttrsBytes) {
        this.sesAttrsBytes = sesAttrsBytes;
    }

    /**
     * @return Session attributes.
     */
    public Map<Object, Object> getSessionAttributes() {
        return sesAttrs;
    }

    /**
     * @return Serialized form of job attributes.
     */
    public byte[] jobAttributesBytes() {
        return jobAttrsBytes;
    }

    /**
     * @param jobAttrsBytes New serialized form of job attributes.
     */
    public void jobAttributesBytes(byte[] jobAttrsBytes) {
        this.jobAttrsBytes = jobAttrsBytes;
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
     * @param cpSpi New checkpoint SPI name.
     */
    public void checkpointSpi(String cpSpi) {
        this.cpSpi = cpSpi;
    }

    /**
     * @return Task local class loader id.
     */
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @param clsLdrId New task local class loader id.
     */
    public void classLoaderId(IgniteUuid clsLdrId) {
        this.clsLdrId = clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode getDeploymentMode() {
        return depModeMsg.value();
    }

    /**
     * @return Deployment mode messsage.
     */
    public DeploymentModeMessage deploymentModeMessage() {
        return depModeMsg;
    }

    /**
     * @param depModeMsg New deployment mode messsage.
     */
    public void deploymentModeMessage(DeploymentModeMessage depModeMsg) {
        this.depModeMsg = depModeMsg;
    }

    /**
     * Returns true if siblings list is dynamic, i.e. task is continuous.
     *
     * @return True if siblings list is dynamic.
     */
    public boolean dynamicSiblings() {
        return dynamicSiblings;
    }

    /**
     * @param dynamicSiblings New dynamic siblings flag.
     */
    public void dynamicSiblings(boolean dynamicSiblings) {
        this.dynamicSiblings = dynamicSiblings;
    }

    /**
     * @return Node class loader participant map.
     */
    public Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParticipants;
    }

    /**
     * @param ldrParticipants New node class loader participant map.
     */
    public void loaderParticipants(Map<UUID, IgniteUuid> ldrParticipants) {
        this.ldrParticipants = ldrParticipants;
    }

    /**
     * @return Returns {@code true} if deployment should always be used.
     */
    public boolean forceLocalDeployment() {
        return forceLocDep;
    }

    /**
     * @param forceLocDep New local deployment forcing flag.
     */
    public void forceLocalDeployment(boolean forceLocDep) {
        this.forceLocDep = forceLocDep;
    }

    /**
     * @return Topology.
     */
    @Nullable public Collection<UUID> topology() {
        return top;
    }

    /**
     * @param top New topology.
     */
    public void topology(@Nullable Collection<UUID> top) {
        this.top = top;
    }

    /**
     * @return Topology predicate.
     */
    public IgnitePredicate<ClusterNode> getTopologyPredicate() {
        return topPred;
    }

    /**
     * @return Marshalled topology predicate.
     */
    public byte[] topologyPredicateBytes() {
        return topPredBytes;
    }

    /**
     * @param topPredBytes New marshalled topology predicate.
     */
    public void topologyPredicateBytes(byte[] topPredBytes) {
        this.topPredBytes = topPredBytes;
    }

    /**
     * @return {@code True} if session attributes are enabled.
     */
    public boolean sessionFullSupport() {
        return sesFullSup;
    }

    /**
     * @param sesFullSup New flag, indicating that session attributes are enabled.
     */
    public void sessionFullSupport(boolean sesFullSup) {
        this.sesFullSup = sesFullSup;
    }

    /**
     * @return {@code True} if internal job.
     */
    public boolean internal() {
        return internal;
    }

    /**
     * @param internal New internal job flag.
     */
    public void internal(boolean internal) {
        this.internal = internal;
    }

    /**
     * @return Caches' identifiers to reserve specified partition for job execution.
     */
    public int[] cacheIds() {
        return cacheIds;
    }

    /**
     * @param cacheIds New cache identifiers.
     */
    public void cacheIds(int[] cacheIds) {
        this.cacheIds = cacheIds;
    }

    /**
     * @return Partition to lock for job execution.
     */
    public int partition() {
        return part;
    }

    /**
     * @param part New partition.
     */
    public void partition(int part) {
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public String executorName() {
        return execName;
    }

    /**
     * @param execName New executor name.
     */
    public void executorName(String execName) {
        this.execName = execName;
    }

    /**
     * @return Affinity version which was used to map job
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
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
