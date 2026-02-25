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
    @Order(0)
    long reqId;

    /** */
    // TODO: Refactor bytes serialization
    @Order(1)
    // TODO: Refactor bytes serialization

    /** Cache name. */
    @Order(2)
    String cacheName;

    /** */
    // TODO: byte[] field - consider refactoring to use typed serialization
    @Order(3)
    byte[] updaterBytes;

    /** Entries to update. */
    @Order(4)
    Collection<DataStreamerEntry> entries;

    /** {@code True} to ignore deployment ownership. */
    @Order(5)
    boolean ignoreDepOwnership;

    /** */
    @Order(6)
    boolean skipStore;

    /** Keep binary flag. */
    @Order(7)
    boolean keepBinary;

    /** */
    // TODO: DeploymentMode enum is serialized as byte ordinal - consider refactoring to use enum serialization
    @Order(8)
    DeploymentMode depMode;

    /** */
    @Order(9)
    String sampleClsName;

    /** */
    @Order(10)
    String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @Order(11)
    Map<UUID, IgniteUuid> ldrParticipants;

    /** */
    @Order(12)
    IgniteUuid clsLdrId;

    /** */
    @Order(13)
    boolean forceLocDep;

    /** Topology version. */
    @Order(14)
    AffinityTopologyVersion topVer;

    /** */
    @Order(15)
    int partId;

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
     * @return Entries to update.
     */
    public Collection<DataStreamerEntry> entries() {
        return entries;
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
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return keepBinary;
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

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Partition ID.
     */
    public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 62;
    }
}
