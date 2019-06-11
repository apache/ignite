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

package org.apache.ignite.internal;

import java.util.BitSet;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;

/**
 * Defines supported features and check its on other nodes.
 */
public enum IgniteFeatures {
    /**
     * Support of {@link HandshakeWaitMessage} by {@link TcpCommunicationSpi}.
     */
    TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE(0),

    /** Cache metrics v2 support. */
    CACHE_METRICS_V2(1),

    /** Data paket compression. */
    DATA_PACKET_COMPRESSION(3),

    /** Support of different rebalance size for nodes.  */
    DIFFERENT_REBALANCE_POOL_SIZE(4),

    /** Support of splitted cache configurations to avoid broken deserialization on non-affinity nodes. */
    SPLITTED_CACHE_CONFIGURATIONS(5),

    /**
     * Support of providing thread dump of thread that started transaction. Used for dumping
     * long running transactions.
     */
    TRANSACTION_OWNER_THREAD_DUMP_PROVIDING(6),

    /** Displaying versbose transaction information: --info option of --tx control script command. */
    TX_INFO_COMMAND(7),

    /** Command which allow to detect and cleanup garbage which could left after destroying caches in shared groups */
    FIND_AND_DELETE_GARBAGE_COMMAND(8),

    /** Distributed metastorage. */
    DISTRIBUTED_METASTORAGE(11),

    /** Supports tracking update counter for transactions. */
    TX_TRACKING_UPDATE_COUNTER(12),

    /** Support new security processor */
    IGNITE_SECURITY_PROCESSOR(13),

    /** Replacing TcpDiscoveryNode field with nodeId field in discovery messages. */
    TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION(14);

    /**
     * Unique feature identifier.
     */
    private final int featureId;

    /**
     * @param featureId Feature ID.
     */
    IgniteFeatures(int featureId) {
        this.featureId = featureId;
    }

    /**
     * @return Feature ID.
     */
    public int getFeatureId() {
        return featureId;
    }

    /**
     * Checks that feature supported by node.
     *
     * @param ctx Kernal context.
     * @param clusterNode Cluster node to check.
     * @param feature Feature to check.
     * @return {@code True} if feature is declared to be supported by remote node.
     */
    public static boolean nodeSupports(GridKernalContext ctx, ClusterNode clusterNode, IgniteFeatures feature) {
        if (ctx != null) {
            RollingUpgradeStatus status = ctx.rollingUpgrade().getStatus();

            if (status.enabled() && !status.forcedModeEnabled())
                return status.supportedFeatures().contains(feature);
        }

        return nodeSupports(clusterNode.attribute(ATTR_IGNITE_FEATURES), feature);
    }

    /**
     * Checks that feature supported by node.
     *
     * @param featuresAttrBytes Byte array value of supported features node attribute.
     * @param feature Feature to check.
     * @return {@code True} if feature is declared to be supported by remote node.
     */
    public static boolean nodeSupports(byte[] featuresAttrBytes, IgniteFeatures feature) {
        if (featuresAttrBytes == null)
            return false;

        int featureId = feature.getFeatureId();

        // Same as "BitSet.valueOf(features).get(featureId)"

        int byteIdx = featureId >>> 3;

        if (byteIdx >= featuresAttrBytes.length)
            return false;

        int bitIdx = featureId & 0x7;

        return (featuresAttrBytes[byteIdx] & (1 << bitIdx)) != 0;
    }

    /**
     * Checks that feature supported by all nodes.
     *
     * @param ctx Kernal context.
     * @param nodes cluster nodes to check their feature support.
     * @return if feature is declared to be supported by all nodes
     */
    public static boolean allNodesSupports(GridKernalContext ctx, Iterable<ClusterNode> nodes, IgniteFeatures feature) {
        if (ctx != null && nodes.iterator().hasNext()) {
            RollingUpgradeStatus status = ctx.rollingUpgrade().getStatus();

            if (status.enabled() && !status.forcedModeEnabled())
                return status.supportedFeatures().contains(feature);
        }

        for (ClusterNode next : nodes) {
            if (!nodeSupports(next.attribute(ATTR_IGNITE_FEATURES), feature))
                return false;
        }

        return true;
    }

    /**
     * Features supported by the current node.
     *
     * @return Byte array representing all supported features by current node.
     */
    public static byte[] allFeatures() {
        final BitSet set = new BitSet();

        for (IgniteFeatures value : IgniteFeatures.values()) {
            // After rolling upgrade, our security has more strict validation. This may come as a surprise to customers.
            if (IGNITE_SECURITY_PROCESSOR == value && !getBoolean(IGNITE_SECURITY_PROCESSOR.name(), true))
                continue;

            final int featureId = value.getFeatureId();

            assert !set.get(featureId) : "Duplicate feature ID found for [" + value + "] having same ID ["
                + featureId + "]";

            set.set(featureId);
        }

        return set.toByteArray();
    }
}
