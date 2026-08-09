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
import org.apache.ignite.internal.DeferredUnmarshalMessage;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.StripedMessage;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.CacheIdAware;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridTopic.TOPIC_DATASTREAM;

/** Batch of streamed entries. The receiver is a user class. */
@UseBinaryMarshaller
public class DataStreamerRequest implements DeferredUnmarshalMessage, CacheIdAware, StripedMessage {
    /** */
    @Order(0)
    long reqId;

    /** */
    @Order(1)
    IgniteUuid resTopicId;

    /** Cache name. */
    @Order(2)
    String cacheName;

    /** Cache receiver. A user object, kept out of the message {@code toString()}. */
    @GridToStringExclude
    @Marshalled("updaterBytes")
    StreamReceiver<?, ?> updater;

    /** Serialized cache receiver. */
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

    /** Empty constructor. */
    public DataStreamerRequest() {
        // No-op.
    }

    /**
     * @param reqId Request ID.
     * @param resTopicId Response topic ID.
     * @param cacheName Cache name.
     * @param updater Cache receiver.
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
        IgniteUuid resTopicId,
        @Nullable String cacheName,
        StreamReceiver<?, ?> updater,
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
        this.resTopicId = resTopicId;
        this.cacheName = cacheName;
        this.updater = updater;
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

    /** @return Request ID. */
    long requestId() {
        return reqId;
    }

    /** @return Response topic. */
    Object responseTopic() {
        return TOPIC_DATASTREAM.topic(resTopicId);
    }

    /** @return Cache name. */
    String cacheName() {
        return cacheName;
    }

    /** @return Updater. */
    StreamReceiver<?, ?> updater() {
        return updater;
    }

    /** @return Serialized updater, {@code null} until the message is marshalled. */
    byte[] updaterBytes() {
        return updaterBytes;
    }

    /**
     * Hands the message the serialized form of its updater, so the marshaller keeps it instead of producing its own.
     * The receiver of a streamer does not change between batches, hence the sender marshals it once and passes the
     * result on.
     *
     * @param updaterBytes Serialized updater taken from an already marshalled request.
     */
    void updaterBytes(byte[] updaterBytes) {
        this.updaterBytes = updaterBytes;
    }

    /** @return Entries to update. */
    Collection<DataStreamerEntry> entries() {
        return entries;
    }

    /** @return {@code True} to ignore ownership. */
    boolean ignoreDeploymentOwnership() {
        return ignoreDepOwnership;
    }

    /** @return Skip store flag. */
    boolean skipStore() {
        return skipStore;
    }

    /** @return Keep binary flag. */
    boolean keepBinary() {
        return keepBinary;
    }

    /** @return Deployment mode. */
    DeploymentMode deploymentMode() {
        return depMode;
    }

    /** @return Sample class name. */
    String sampleClassName() {
        return sampleClsName;
    }

    /** @return User version. */
    String userVersion() {
        return userVer;
    }

    /** @return Participants. */
    Map<UUID, IgniteUuid> participants() {
        return ldrParticipants;
    }

    /** @return Class loader ID. */
    IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /** @return {@code True} to force local deployment. */
    boolean forceLocalDeployment() {
        return forceLocDep;
    }

    /** @return Topology version. */
    AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public int stripeIdx() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return GridCacheUtils.cacheId(cacheName);
    }
}
