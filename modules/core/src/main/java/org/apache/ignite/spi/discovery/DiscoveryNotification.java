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
package org.apache.ignite.spi.discovery;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.tracing.messages.SpanContainer;
import org.jetbrains.annotations.Nullable;

/**
 * Discovery notification object.
 */
public class DiscoveryNotification {
    /** Event type. */
    private final int eventType;

    /** Topology version. */
    private final long topVer;

    /** Node. */
    private final ClusterNode node;

    /** Topology snapshot. */
    private final Collection<ClusterNode> topSnapshot;

    /** Topology history. */
    private @Nullable Map<Long, Collection<ClusterNode>> topHist;

    /** Custom message data. */
    private @Nullable DiscoverySpiCustomMessage customMsgData;

    /** Span container. */
    private SpanContainer spanContainer;

    /**
     * @param eventType Event type.
     * @param topVer Topology version.
     * @param node Node.
     * @param topSnapshot Topology snapshot.
     */
    public DiscoveryNotification(int eventType, long topVer, ClusterNode node, Collection<ClusterNode> topSnapshot) {
        this.eventType = eventType;
        this.topVer = topVer;
        this.node = node;
        this.topSnapshot = topSnapshot;
    }

    /**
     * @param eventType Event type.
     * @param topVer Topology version.
     * @param node Node.
     * @param topSnapshot Topology snapshot.
     * @param topHist Topology history.
     * @param customMsgData Custom message data.
     * @param spanContainer Span container.
     */
    public DiscoveryNotification(
        int eventType,
        long topVer,
        ClusterNode node,
        Collection<ClusterNode> topSnapshot,
        @Nullable Map<Long, Collection<ClusterNode>> topHist,
        @Nullable DiscoverySpiCustomMessage customMsgData,
        SpanContainer spanContainer
    ) {
        this.eventType = eventType;
        this.topVer = topVer;
        this.node = node;
        this.topSnapshot = topSnapshot;
        this.topHist = topHist;
        this.customMsgData = customMsgData;
        this.spanContainer = spanContainer;
    }

    /**
     * @return Event type.
     */
    public int type() {
        return eventType;
    }

    /**
     * @return Topology version.
     */
    public long getTopVer() {
        return topVer;
    }

    /**
     * @return Cluster node.
     */
    public ClusterNode getNode() {
        return node;
    }

    /**
     * @return Topology snapshot.
     */
    public Collection<ClusterNode> getTopSnapshot() {
        return topSnapshot;
    }

    /**
     * @return Topology history.
     */
    public Map<Long, Collection<ClusterNode>> getTopHist() {
        return topHist;
    }

    /**
     * @return Custom message data.
     */
    public DiscoverySpiCustomMessage getCustomMsgData() {
        return customMsgData;
    }

    /**
     * @return Span container.
     */
    public SpanContainer getSpanContainer() {
        return spanContainer;
    }
}
