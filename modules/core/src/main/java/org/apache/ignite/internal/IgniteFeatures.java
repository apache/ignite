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
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;

/**
 * Defines supported features and check its on other nodes.
 */
public enum IgniteFeatures {
    /**
     * Support of {@link HandshakeWaitMessage} by {@link TcpCommunicationSpi}.
     */
    TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE(0);

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
     * @return {@code True} if feature is declared to be supported by remote node.
     */
    public static boolean nodeSupports(ClusterNode clusterNode, IgniteFeatures feature) {
        final byte[] features = clusterNode.attribute(ATTR_IGNITE_FEATURES);

        return features != null && BitSet.valueOf(features).get(feature.getFeatureId());
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
     * Features supported by the current node.
     *
     * @return Byte array representing all supported features by current node.
     */
    public static byte[] allFeatures() {
        final BitSet set = new BitSet();

        for (IgniteFeatures value : IgniteFeatures.values()) {
            final int featureId = value.getFeatureId();

            assert !set.get(featureId) : "Duplicate feature ID found for [" + value + "] having same ID ["
                + featureId + "]";

            set.set(featureId);
        }

        return set.toByteArray();
    }
}
