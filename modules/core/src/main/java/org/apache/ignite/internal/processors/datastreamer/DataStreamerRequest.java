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
import org.apache.ignite.internal.DeferredUnmarshalMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.StripedMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.CacheIdAware;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridTopic.TOPIC_DATASTREAM;

/** Batch of streamed entries. */
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

    /** Cache updater; {@code null} when {@link #builtInUpdater} names it. */
    @GridToStringExclude
    @Order(3)
    DataStreamerReceiverMessage updaterMsg;

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

    /** Deployment of the streamed classes. */
    @Order(8)
    GridDeploymentInfoMessage depInfo;

    /** */
    @Order(9)
    String sampleClsName;

    /** */
    @Order(10)
    boolean forceLocDep;

    /** Topology version. */
    @Order(11)
    AffinityTopologyVersion topVer;

    /** */
    @Order(12)
    int partId;

    /** Cache updater of the streamer itself; {@code null} when {@link #updaterMsg} carries a user one. */
    @Order(13)
    DataStreamerBuiltInUpdater builtInUpdater;

    /** Empty constructor. */
    public DataStreamerRequest() {
        // No-op.
    }

    /**
     * @param reqId Request ID.
     * @param resTopicId Response topic ID.
     * @param cacheName Cache name.
     * @param updaterMsg Cache updater, {@code null} when {@code builtInUpdater} names it.
     * @param builtInUpdater Cache updater of the streamer itself, {@code null} for a user one.
     * @param entries Entries to put.
     * @param ignoreDepOwnership Ignore ownership.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param depInfo Deployment of the streamed classes.
     * @param sampleClsName Sample class name.
     * @param forceLocDep Force local deployment.
     * @param topVer Topology version.
     * @param partId Partition ID.
     */
    public DataStreamerRequest(
        long reqId,
        IgniteUuid resTopicId,
        @Nullable String cacheName,
        @Nullable DataStreamerReceiverMessage updaterMsg,
        @Nullable DataStreamerBuiltInUpdater builtInUpdater,
        Collection<DataStreamerEntry> entries,
        boolean ignoreDepOwnership,
        boolean skipStore,
        boolean keepBinary,
        GridDeploymentInfo depInfo,
        String sampleClsName,
        boolean forceLocDep,
        @NotNull AffinityTopologyVersion topVer,
        int partId
    ) {
        assert topVer != null;

        this.reqId = reqId;
        this.resTopicId = resTopicId;
        this.cacheName = cacheName;
        this.updaterMsg = updaterMsg;
        this.builtInUpdater = builtInUpdater;
        this.entries = entries;
        this.ignoreDepOwnership = ignoreDepOwnership;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.depInfo = depInfo != null ? new GridDeploymentInfoMessage(depInfo) : null;
        this.sampleClsName = sampleClsName;
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

    /** @return Updater: the one carried by the request, or the streamer's own that it named. */
    StreamReceiver<?, ?> updater() {
        return updaterMsg != null ? updaterMsg.receiver() : builtInUpdater.updater();
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

    /** @return Deployment of the streamed classes. */
    GridDeploymentInfo deploymentInfo() {
        return depInfo;
    }

    /** @return Sample class name. */
    String sampleClassName() {
        return sampleClsName;
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
