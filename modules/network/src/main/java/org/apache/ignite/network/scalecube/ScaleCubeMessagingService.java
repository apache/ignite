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
import java.util.concurrent.CompletionException;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.AbstractMessagingService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
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
     * Sets the ScaleCube's {@link Cluster}. Needed for cyclic dependency injection.
     *
     * @param cluster Cluster.
     */
    void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Delegates the received message to the registered message handlers.
     *
     * @param message Received message.
     */
    void fireEvent(Message message) {
        NetworkMessage msg = message.data();

        var address = NetworkAddress.from(message.header(Message.HEADER_SENDER));

        String correlationId = message.correlationId();

        for (NetworkMessageHandler handler : getMessageHandlers(msg.groupType()))
            handler.onReceived(msg, address, correlationId);
    }

    /** {@inheritDoc} */
    @Override public void weakSend(ClusterNode recipient, NetworkMessage msg) {
        cluster
            .send(fromNetworkAddress(recipient.address()), Message.fromData(msg))
            .subscribe();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg) {
        // TODO: IGNITE-15161 Temporarly, probably should be removed after the implementation
        // TODO of stopping the clusterService cause some sort of stop thread-safety logic will be implemented.
        if (cluster.isShutdown()) {
            CompletableFuture nodeStoppingRes = new CompletableFuture<NetworkMessage>();
            nodeStoppingRes.completeExceptionally(new NodeStoppingException());

            return nodeStoppingRes;
        }

        return cluster
            .send(fromNetworkAddress(recipient.address()), Message.fromData(msg))
            .toFuture();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg, String correlationId) {
        return send(recipient.address(), msg, correlationId);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> send(NetworkAddress addr, NetworkMessage msg, String correlationId) {
        // TODO: IGNITE-15161 Temporarly, probably should be removed after the implementation
        // TODO of stopping the clusterService cause some sort of stop thread-safety logic will be implemented.
        if (cluster.isShutdown()) {
            CompletableFuture nodeStoppingRes = new CompletableFuture<NetworkMessage>();
            nodeStoppingRes.completeExceptionally(new NodeStoppingException());

            return nodeStoppingRes;
        }

        var message = Message
            .withData(msg)
            .correlationId(correlationId)
            .build();

        return cluster
            .send(fromNetworkAddress(addr), message)
            .toFuture();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, NetworkMessage msg, long timeout) {
        return invoke(recipient.address(), msg, timeout);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<NetworkMessage> invoke(NetworkAddress addr, NetworkMessage msg, long timeout) {
        // TODO: IGNITE-15161 Temporarly, probably should be removed after the implementation
        // TODO of stopping the clusterService cause some sort of stop thread-safety logic will be implemented.
        if (cluster.isShutdown()) {
            CompletableFuture nodeStoppingRes = new CompletableFuture<NetworkMessage>();
            nodeStoppingRes.completeExceptionally(new NodeStoppingException());

            return nodeStoppingRes;
        }

        var message = Message
            .withData(msg)
            .correlationId(UUID.randomUUID().toString())
            .build();

        // TODO: IGNITE-15196 Null seems to be an unexpected result on node stopping.
        return cluster
            .requestResponse(fromNetworkAddress(addr), message)
            .timeout(Duration.ofMillis(timeout))
            .toFuture()
            .thenApply(m -> {
                if (m == null)
                    throw new CompletionException(new NodeStoppingException());
                else
                    return m.data();
            }); // The result can be null on node stopping.
    }

    /**
     * Converts a {@link NetworkAddress} into ScaleCube's {@link Address}.
     */
    private static Address fromNetworkAddress(NetworkAddress address) {
        return Address.create(address.host(), address.port());
    }
}
