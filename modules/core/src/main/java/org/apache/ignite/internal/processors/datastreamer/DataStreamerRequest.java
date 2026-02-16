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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DataStreamerRequest implements Message {
    /** */
    @Order(value = 0, method = "requestId")
    private long reqId;

    /** */
    // TODO: byte[] field - consider refactoring to use typed serialization
    @Order(value = 1, method = "responseTopicBytes")
    private byte[] resTopicBytes;

    /** Cache name. */
    @Order(2)
    private String cacheName;

    /** */
    // TODO: byte[] field - consider refactoring to use typed serialization
    @Order(3)
    private byte[] updaterBytes;

    /** Entries to update. */
    @Order(4)
    private Collection<DataStreamerEntry> entries;

    /** {@code True} to ignore deployment ownership. */
    @Order(value = 5, method = "ignoreDeploymentOwnership")
    private boolean ignoreDepOwnership;

    /** */
    @Order(6)
    private boolean skipStore;

    /** Keep binary flag. */
    @Order(7)
    private boolean keepBinary;

    /** */
    // TODO: DeploymentMode enum is serialized as byte ordinal - consider refactoring to use enum serialization
    @Order(value = 8, method = "deploymentMode")
    private DeploymentMode depMode;

    /** */
    @Order(value = 9, method = "sampleClassName")
    private String sampleClsName;

    /** */
    @Order(value = 10, method = "userVersion")
    private String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @Order(value = 11, method = "participants")
    private Map<UUID, IgniteUuid> ldrParticipants;

    /** */
    @Order(value = 12, method = "classLoaderId")
    private IgniteUuid clsLdrId;

    /** */
    @Order(value = 13, method = "forceLocalDeployment")
    private boolean forceLocDep;

    /** Topology version. */
    @Order(value = 14, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** */
    @Order(value = 15, method = "partition")
    private int partId;

    /**
     * Empty constructor.
     */
    public DataStreamerRequest() {
        // No-op.
    }

    /**
     * @param reqId Request ID.
     * @param resTopicBytes Response topic.
     * @param cacheName Cache name.
     * @param updaterBytes Cache receiver.
     * @param entries Entries to put.
     * @param ignoreDepOwnership Ignore ownership.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param depMode Deployment mode.
     * @param sampleClsName Sample class name.
     * @param userVer User version.
     * @param ldrParticipants Loader participants.
     * @param clsLdrId Class loader ID.
     * @param forceLocDep Force local deployment.
     * @param topVer Topology version.
     * @param partId Partition ID.
     */
    public DataStreamerRequest(
        long reqId,
        byte[] resTopicBytes,
        @Nullable String cacheName,
        byte[] updaterBytes,
        Collection<DataStreamerEntry> entries,
        boolean ignoreDepOwnership,
        boolean skipStore,
        boolean keepBinary,
        DeploymentMode depMode,
        String sampleClsName,
        String userVer,
        Map<UUID, IgniteUuid> ldrParticipants,
        IgniteUuid clsLdrId,
        boolean forceLocDep,
        @NotNull AffinityTopologyVersion topVer,
        int partId
    ) {
        assert topVer != null;

        this.reqId = reqId;
        this.resTopicBytes = resTopicBytes;
        this.cacheName = cacheName;
        this.updaterBytes = updaterBytes;
        this.entries = entries;
        this.ignoreDepOwnership = ignoreDepOwnership;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.depMode = depMode;
        this.sampleClsName = sampleClsName;
        this.userVer = userVer;
        this.ldrParticipants = ldrParticipants;
        this.clsLdrId = clsLdrId;
        this.forceLocDep = forceLocDep;
        this.topVer = topVer;
        this.partId = partId;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param reqId Request ID.
     */
    public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Response topic.
     */
    public byte[] responseTopicBytes() {
        return resTopicBytes;
    }

    /**
     * @param resTopicBytes Response topic bytes.
     */
    public void responseTopicBytes(byte[] resTopicBytes) {
        this.resTopicBytes = resTopicBytes;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Updater.
     */
    public byte[] updaterBytes() {
        return updaterBytes;
    }

    /**
     * @param updaterBytes Updater bytes.
     */
    public void updaterBytes(byte[] updaterBytes) {
        this.updaterBytes = updaterBytes;
    }

    /**
     * @return Entries to update.
     */
    public Collection<DataStreamerEntry> entries() {
        return entries;
    }

    /**
     * @param entries Entries to update.
     */
    public void entries(Collection<DataStreamerEntry> entries) {
        this.entries = entries;
    }

    /**
     * @return {@code True} to ignore ownership.
     */
    public boolean ignoreDeploymentOwnership() {
        return ignoreDepOwnership;
    }

    /**
     * @param ignoreDepOwnership Ignore deployment ownership flag.
     */
    public void ignoreDeploymentOwnership(boolean ignoreDepOwnership) {
        this.ignoreDepOwnership = ignoreDepOwnership;
    }

    /**
     * @return Skip store flag.
     */
    public boolean skipStore() {
        return skipStore;
    }

    /**
     * @param skipStore Skip store flag.
     */
    public void skipStore(boolean skipStore) {
        this.skipStore = skipStore;
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @param keepBinary Keep binary flag.
     */
    public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @param depMode Deployment mode.
     */
    public void deploymentMode(DeploymentMode depMode) {
        this.depMode = depMode;
    }

    /**
     * @return Sample class name.
     */
    public String sampleClassName() {
        return sampleClsName;
    }

    /**
     * @param sampleClsName Sample class name.
     */
    public void sampleClassName(String sampleClsName) {
        this.sampleClsName = sampleClsName;
    }

    /**
     * @return User version.
     */
    public String userVersion() {
        return userVer;
    }

    /**
     * @param userVer User version.
     */
    public void userVersion(String userVer) {
        this.userVer = userVer;
    }

    /**
     * @return Participants.
     */
    public Map<UUID, IgniteUuid> participants() {
        return ldrParticipants;
    }

    /**
     * @param ldrParticipants Loader participants.
     */
    public void participants(Map<UUID, IgniteUuid> ldrParticipants) {
        this.ldrParticipants = ldrParticipants;
    }

    /**
     * @return Class loader ID.
     */
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @param clsLdrId Class loader ID.
     */
    public void classLoaderId(IgniteUuid clsLdrId) {
        this.clsLdrId = clsLdrId;
    }

    /**
     * @return {@code True} to force local deployment.
     */
    public boolean forceLocalDeployment() {
        return forceLocDep;
    }

    /**
     * @param forceLocDep Force local deployment flag.
     */
    public void forceLocalDeployment(boolean forceLocDep) {
        this.forceLocDep = forceLocDep;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Partition ID.
     */
    public int partition() {
        return partId;
    }

    /**
     * @param partId Partition ID.
     */
    public void partition(int partId) {
        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 62;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerRequest.class, this);
    }
}
