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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.discovery.DiscoverySpi;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PME_FREE_SWITCH_DISABLED;
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

    /** Support of cluster read-only mode. */
    CLUSTER_READ_ONLY_MODE(9),

    /** Support of suspend/resume operations for pessimistic transactions. */
    SUSPEND_RESUME_PESSIMISTIC_TX(10),

    /** Distributed metastorage. */
    DISTRIBUTED_METASTORAGE(11),

    /** The node can communicate with others via socket channel. */
    CHANNEL_COMMUNICATION(12),

    /** Replacing TcpDiscoveryNode field with nodeId field in discovery messages. */
    TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION(14),

    /** LRT system and user time dump settings.  */
    LRT_SYSTEM_USER_TIME_DUMP_SETTINGS(18),

    /** Partition Map Exchange-free switch on baseline node left at fully rebalanced cluster.  */
    PME_FREE_SWITCH(19),

    /** Master key change. See {@link GridEncryptionManager#changeMasterKey(String)}. */
    MASTER_KEY_CHANGE(20),

    /** ContinuousQuery with security subject id support. */
    CONT_QRY_SECURITY_AWARE(21),

    /**
     * Preventing loss of in-memory data when deactivating the cluster.
     *
     * @see ClusterState#INACTIVE
     */
    SAFE_CLUSTER_DEACTIVATION(22),

    /** Persistence caches can be snapshot.  */
    PERSISTENCE_CACHE_SNAPSHOT(23),

    /** Tracing. */
    TRACING(26),

    /** Distributed change timeout for dump long operations. */
    DISTRIBUTED_CHANGE_LONG_OPERATIONS_DUMP_TIMEOUT(30),

    /** Check secondary indexes inline size on join/by control utility request. */
    CHECK_INDEX_INLINE_SIZES(36),

    /** Distributed propagation of tx collisions dump interval. */
    DISTRIBUTED_TX_COLLISIONS_DUMP(37),

    /** Remove metadata from cluster for specified type. */
    REMOVE_METADATA(39),

    /** Support policy of shutdown. */
    SHUTDOWN_POLICY(40),

    /** Force rebuild, list or request indexes rebuild status from control script. */
    INDEXES_MANIPULATIONS_FROM_CONTROL_SCRIPT(42),

    /** Optimization of recovery protocol for cluster which doesn't contain MVCC caches. */
    MVCC_TX_RECOVERY_PROTOCOL_V2(44),

    /** Pk index keys are applied in correct order. */
    SPECIFIED_SEQ_PK_KEYS(45),

    /** Compatibility support for new fields which are configured split. */
    SPLITTED_CACHE_CONFIGURATIONS_V2(46);

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
     * Check that feature is supported by all remote nodes.
     *
     * @param discoSpi Discovery SPI implementation.
     * @param feature Feature to check.
     * @return {@code True} if all remote nodes support the feature.
     */
    public static boolean allNodesSupport(
        DiscoverySpi discoSpi,
        IgniteFeatures feature
    ) {
        if (discoSpi instanceof IgniteDiscoverySpi)
            return ((IgniteDiscoverySpi)discoSpi).allNodesSupport(feature);
        else
            return allNodesSupports(discoSpi.getRemoteNodes(), feature);
    }

    /**
     * Features supported by the current node.
     *
     * @return Byte array representing all supported features by current node.
     */
    public static byte[] allFeatures() {
        final BitSet set = new BitSet();

        for (IgniteFeatures value : IgniteFeatures.values()) {
            if (value == PME_FREE_SWITCH && getBoolean(IGNITE_PME_FREE_SWITCH_DISABLED))
                continue;

            final int featureId = value.getFeatureId();

            assert !set.get(featureId) : "Duplicate feature ID found for [" + value + "] having same ID ["
                + featureId + "]";

            set.set(featureId);
        }

        return set.toByteArray();
    }
}
