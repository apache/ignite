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

package org.apache.ignite.network.scalecube;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import org.apache.ignite.network.AbstractMessagingService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;

/**
 * Implementation of {@link MessagingService} based on ScaleCube.
 */
final class ScaleCubeMessagingService extends AbstractMessagingService {
    /**
     * Inner representation of a ScaleCube cluster.
     */
    private Cluster cluster;

    /**
     * Topology service.
     */
    private ScaleCubeTopologyService topologyService;

    /** */
    ScaleCubeMessagingService(ScaleCubeTopologyService topologyService) {
        this.topologyService = topologyService;
    }

    /**
     * Sets the ScaleCube's {@link Cluster}. Needed for cyclic dependency injection.
     */
    void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Delegates the received message to the registered message handlers.
     */
    void fireEvent(Message message) {
        NetworkMessage msg = message.data();
        ClusterNode sender = topologyService.getByAddress(message.header(Message.HEADER_SENDER));

        if (sender == null) // Ignore the message from the unknown node.
            return;

        String correlationId = message.correlationId();
        for (NetworkMessageHandler handler : getMessageHandlers())
            handler.onReceived(msg, sender, correlationId);
    }

    /** {@inheritDoc} */
    @Override public void weakSend(ClusterNode recipient, NetworkMessage msg) {
        cluster
            .send(clusterNodeAddress(recipient), Message.fromData(msg))
            .subscribe();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg) {
        return cluster
            .send(clusterNodeAddress(recipient), Message.fromData(msg))
            .toFuture();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg, String correlationId) {
        var message = Message
            .withData(msg)
            .correlationId(correlationId)
            .build();
        return cluster
            .send(clusterNodeAddress(recipient), message)
            .toFuture();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, NetworkMessage msg, long timeout) {
        var message = Message
            .withData(msg)
            .correlationId(UUID.randomUUID().toString())
            .build();
        return cluster
            .requestResponse(clusterNodeAddress(recipient), message)
            .timeout(Duration.ofMillis(timeout))
            .toFuture()
            .thenApply(Message::data);
    }

    /**
     * Extracts the given node's {@link Address}.
     */
    private static Address clusterNodeAddress(ClusterNode node) {
        return Address.create(node.host(), node.port());
    }
}
