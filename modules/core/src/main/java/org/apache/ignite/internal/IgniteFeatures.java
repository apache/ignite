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

import java.util.BitSet;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.discovery.DiscoverySpi;

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

    /**
     * Support of providing thread dump of thread that started transaction. Used for dumping
     * long running transactions.
     */
    TRANSACTION_OWNER_THREAD_DUMP_PROVIDING(6),


    /** Displaying versbose transaction information: --info option of --tx control script command. */
    TX_INFO_COMMAND(7),

    /** Command which allow to detect and cleanup garbage which could left after destroying caches in shared groups */
    FIND_AND_DELETE_GARBAGE_COMMAND(8),

    /** Supports tracking update counter for transactions. */
    TX_TRACKING_UPDATE_COUNTER(12),

    /** Distributed metastorage. */
    IGNITE_SECURITY_PROCESSOR(13),

    /** LRT system and user time dump settings.  */
    LRT_SYSTEM_USER_TIME_DUMP_SETTINGS(18),

    /**
     * A mode when data nodes throttle update rate regarding to DR sender load
     */
    DR_DATA_NODE_SMART_THROTTLING(19),

    /** Support of DR events from  Web Console. */
    WC_DR_EVENTS(20),

    /** Support of chain parameter in snapshot delete task for Web Console. */
    WC_SNAPSHOT_CHAIN_MODE(22),

    /** Support of DR-specific visor tasks used by control utility. */
    DR_CONTROL_UTILITY(25),

    /** Distributed change timeout for dump long operations. */
    DISTRIBUTED_CHANGE_LONG_OPERATIONS_DUMP_TIMEOUT(30),

    /** Cluster has task to get value from cache by key value. */
    WC_GET_CACHE_VALUE(31),

    /** */
    VOLATILE_DATA_STRUCTURES_REGION(33),

    /** Partition reconciliation utility. */
    PARTITION_RECONCILIATION(34);

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
     * @param clusterNode Cluster node to check.
     * @param feature Feature to check.
     * @return {@code True} if feature is declared to be supported by remote node.
     */
    public static boolean nodeSupports(ClusterNode clusterNode, IgniteFeatures feature) {
        final byte[] features = clusterNode.attribute(ATTR_IGNITE_FEATURES);

        if (features == null)
            return false;

        return nodeSupports(features, feature);
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
     * @param nodes cluster nodes to check their feature support.
     * @return if feature is declared to be supported by all nodes
     */
    public static boolean allNodesSupports(Iterable<ClusterNode> nodes, IgniteFeatures feature) {
        for (ClusterNode next : nodes) {
            if (!nodeSupports(next, feature))
                return false;
        }

        return true;
    }

    /**
     * @param cfg Configuration.
     * @param feature Feature to check.
     *
     * @return {@code True} if all nodes in the cluster support given feature.
     */
    public static boolean allNodesSupport(IgniteConfiguration cfg, IgniteFeatures feature) {
        DiscoverySpi discoSpi = cfg.getDiscoverySpi();

        if (discoSpi instanceof IgniteDiscoverySpi)
            return ((IgniteDiscoverySpi)discoSpi).allNodesSupport(feature);
        else {
            Collection<ClusterNode> nodes = discoSpi.getRemoteNodes();

            return allNodesSupports(nodes, feature);
        }
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
            if (IGNITE_SECURITY_PROCESSOR == value && !getBoolean(IGNITE_SECURITY_PROCESSOR.name(), false))
                continue;

            final int featureId = value.getFeatureId();

            assert !set.get(featureId) : "Duplicate feature ID found for [" + value + "] having same ID ["
                + featureId + "]";

            set.set(featureId);
        }

        return set.toByteArray();
    }
}
