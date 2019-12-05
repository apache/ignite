/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent;

import java.util.UUID;

/**
 * Stomp destinations utils.
 */
public class StompDestinationsUtils {
    /** Cluster topology destination. */
    private static final String CLUSTER_PREFIX_DEST = "/app/agent/cluster";

    /** Cluster topology destination. */
    private static final String CLUSTER_TOPOLOGY_DEST = CLUSTER_PREFIX_DEST + "/topology/";

    /** Cluster caches info destination. */
    private static final String CLUSTER_CACHES_INFO_DEST = CLUSTER_PREFIX_DEST + "/caches/";

    /** Cluster caches sql meta destination. */
    private static final String CLUSTER_CACHES_SQL_META_DEST = CLUSTER_CACHES_INFO_DEST + "sql-meta/";

    /** Cluster node configuration. */
    private static final String CLUSTER_NODE_CONFIGURATION = CLUSTER_PREFIX_DEST + "/node-config/";

    /** Cluster action response destination. */
    private static final String CLUSTER_ACTION_RESPONSE_DEST = CLUSTER_PREFIX_DEST + "/action/";

    /** Span destination. */
    private static final String SPAN_DEST = "/app/agent/spans/";

    /** Metrics destination. */
    private static final String METRICS_DEST = "/app/agent/metrics/add";

    /** Events destination. */
    private static final String EVENTS_DEST = CLUSTER_PREFIX_DEST + "/events/";

    /**
     * @param clusterId Cluster id.
     * @return Cluster topology destination.
     */
    public static String buildClusterTopologyDest(UUID clusterId) {
        return CLUSTER_TOPOLOGY_DEST + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Cluster node configuration.
     */
    public static String buildClusterNodeConfigurationDest(UUID clusterId) {
        return CLUSTER_NODE_CONFIGURATION + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Save span destination.
     */
    public static String buildSaveSpanDest(UUID clusterId) {
        return SPAN_DEST + clusterId + "/add";
    }

    /**
     * @return Metrics destination.
     */
    public static String buildMetricsDest() {
        return METRICS_DEST;
    }

    /**
     * @return Events destination.
     */
    public static String buildEventsDest(UUID clusterId) {
        return EVENTS_DEST + clusterId + "/add";
    }

    /**
     * @param clusterId Cluster id.
     * @param resId Response id.
     * @return Action response destination.
     */
    public static String buildActionResponseDest(UUID clusterId, UUID resId) {
        return CLUSTER_ACTION_RESPONSE_DEST + clusterId + "/" + resId;
    }

    /**
     * @return Metrics pull topic.
     */
    public static String buildMetricsPullTopic() {
        return "/topic/agent/metrics/pull";
    }

    /**
     * @param clusterId Cluster id.
     * @return Action request topic.
     */
    public static String buildActionRequestTopic(UUID clusterId) {
        return "/topic/agent/cluster/action/" + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Send cluster info destination.
     */
    public static String buildClusterDest(UUID clusterId) {
        return "/app/agent/cluster/" + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Cluster caches information destination.
     */
    public static String buildClusterCachesInfoDest(UUID clusterId) {
        return CLUSTER_CACHES_INFO_DEST + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Cluster caches sql metadata destination.
     */
    public static String buildClusterCachesSqlMetaDest(UUID clusterId) {
        return CLUSTER_CACHES_SQL_META_DEST + clusterId;
    }
}
