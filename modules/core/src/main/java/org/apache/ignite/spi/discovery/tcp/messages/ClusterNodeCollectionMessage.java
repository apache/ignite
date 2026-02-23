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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

/** A container message for a collection of {@link TcpDiscoveryNodeMessage}. */
public class ClusterNodeCollectionMessage implements TcpDiscoveryMarshallableMessage {
    /** The collection of wrapped {@link TcpDiscoveryNodeMessage}. */
    @Order(value = 0, method = "clusterNodeMessages")
    private Collection<TcpDiscoveryNodeMessage> clusterNodeMsgs;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public ClusterNodeCollectionMessage() {
        // No-op.
    }

    /** @param clusterNodeMsgs Holder messages of {@link ClusterNode}. */
    public ClusterNodeCollectionMessage(Collection<TcpDiscoveryNodeMessage> clusterNodeMsgs) {
        this.clusterNodeMsgs = clusterNodeMsgs;
    }

    /**
     * @param clusterNodes Tcp discovery nodes.
     * @return {@link ClusterNodeCollectionMessage}
     */
    public static ClusterNodeCollectionMessage of(Collection<TcpDiscoveryNode> clusterNodes) {
        return new ClusterNodeCollectionMessage(clusterNodes.stream().map(TcpDiscoveryNodeMessage::new).collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        clusterNodeMsgs.forEach(m -> finishUnmarshal(marsh, clsLdr));
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) {
        clusterNodeMsgs.forEach(m -> prepareMarshal(marsh));
    }

    /** @return Holder messages of {@link ClusterNode}. */
    public Collection<TcpDiscoveryNodeMessage> clusterNodeMessages() {
        return clusterNodeMsgs;
    }

    /** @return Collection of {@link ClusterNode}. */
    public Collection<ClusterNode> clusterNodes() {
        return clusterNodeMsgs.stream().map(msg -> (ClusterNode)msg).collect(Collectors.toList());
    }

    /** @param clusterNodeMsgs Holder messages of {@link ClusterNode}. */
    public void clusterNodeMessages(Collection<TcpDiscoveryNodeMessage> clusterNodeMsgs) {
        this.clusterNodeMsgs = clusterNodeMsgs;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -110;
    }
}
