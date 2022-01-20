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

package org.apache.ignite.network;

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.message.InvokeRequest;
import org.apache.ignite.internal.network.message.InvokeResponse;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/** Default messaging service implementation. */
public class DefaultMessagingService extends AbstractMessagingService {
    /** Network messages factory. */
    private final NetworkMessagesFactory factory;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Connection manager that provides access to {@link NettySender}. */
    private volatile ConnectionManager connectionManager;

    /**
     * This node's local socket address. Not volatile, because access is guarded by volatile reads/writes to the {@code connectionManager}
     * variable.
     */
    private InetSocketAddress localAddress;

    /** Collection that maps correlation id to the future for an invocation request. */
    private final ConcurrentMap<Long, CompletableFuture<NetworkMessage>> requestsMap = new ConcurrentHashMap<>();

    /** Correlation id generator. */
    private final AtomicLong correlationIdGenerator = new AtomicLong();

    /** Fake host for nodes that are not in the topology yet. TODO: IGNITE-16373 Remove after the ticket is resolved. */
    private static final String UNKNOWN_HOST = "unknown";

    /** Fake port for nodes that are not in the topology yet. TODO: IGNITE-16373 Remove after the ticket is resolved. */
    private static final int UNKNOWN_HOST_PORT = 1337;

    /**
     * Constructor.
     *
     * @param factory Network messages factory.
     * @param topologyService Topology service.
     */
    public DefaultMessagingService(NetworkMessagesFactory factory, TopologyService topologyService) {
        this.factory = factory;
        this.topologyService = topologyService;
    }

    /**
     * Resolves cyclic dependency and sets up the connection manager.
     *
     * @param connectionManager Connection manager.
     */
    public void setConnectionManager(ConnectionManager connectionManager) {
        this.localAddress = (InetSocketAddress) connectionManager.getLocalAddress();
        this.connectionManager = connectionManager;
        connectionManager.addListener(this::onMessage);
    }

    /** {@inheritDoc} */
    @Override
    public void weakSend(ClusterNode recipient, NetworkMessage msg) {
        send(recipient, msg);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg) {
        return send0(recipient, recipient.address(), msg, null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> respond(ClusterNode recipient, NetworkMessage msg, long correlationId) {
        return send0(recipient, recipient.address(), msg, correlationId);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> respond(NetworkAddress addr, NetworkMessage msg, long correlationId) {
        ClusterNode recipient = topologyService.getByAddress(addr);
        return send0(recipient, addr, msg, correlationId);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, NetworkMessage msg, long timeout) {
        return invoke0(recipient, recipient.address(), msg, timeout);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NetworkMessage> invoke(NetworkAddress addr, NetworkMessage msg, long timeout) {
        ClusterNode recipient = topologyService.getByAddress(addr);

        return invoke0(recipient, addr, msg, timeout);
    }

    /**
     * Sends a message. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node. TODO: Maybe {@code null} due to IGNITE-16373.
     * @param address Target address.
     * @param msg Message.
     * @param correlationId Correlation id. Not null iff the message is a response to a {@link #invoke} request.
     * @return Future of the send operation.
     */
    private CompletableFuture<Void> send0(@Nullable ClusterNode recipient, NetworkAddress address, NetworkMessage msg,
            @Nullable Long correlationId) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        InetSocketAddress addr = new InetSocketAddress(address.host(), address.port());

        if (isSelf(recipient, address.consistentId(), addr)) {
            if (correlationId != null) {
                onInvokeResponse(msg, correlationId);
            } else {
                sendToSelf(msg, null);
            }

            return CompletableFuture.completedFuture(null);
        }

        NetworkMessage message = correlationId != null ? responseFromMessage(msg, correlationId) : msg;

        String recipientConsistentId = recipient != null ? recipient.name() : address.consistentId();

        return connectionManager.channel(recipientConsistentId, addr).thenCompose(sender -> sender.send(message));
    }

    /**
     * Sends an invocation request. If the target is the current node, then message will be delivered immediately.
     *
     * @param recipient Target cluster node. TODO: Maybe {@code null} due to IGNITE-16373.
     * @param addr Target address.
     * @param msg Message.
     * @param timeout Invocation timeout.
     * @return A future holding the response or error if the expected response was not received.
     */
    private CompletableFuture<NetworkMessage> invoke0(@Nullable ClusterNode recipient, NetworkAddress addr,
            NetworkMessage msg, long timeout) {
        if (connectionManager.isStopped()) {
            return failedFuture(new NodeStoppingException());
        }

        long correlationId = createCorrelationId();

        CompletableFuture<NetworkMessage> responseFuture = new CompletableFuture<NetworkMessage>()
                .orTimeout(timeout, TimeUnit.MILLISECONDS);

        requestsMap.put(correlationId, responseFuture);

        InetSocketAddress address = new InetSocketAddress(addr.host(), addr.port());

        if (isSelf(recipient, addr.consistentId(), address)) {
            sendToSelf(msg, correlationId);
            return responseFuture;
        }

        InvokeRequest message = requestFromMessage(msg, correlationId);

        String recipientConsistentId = recipient != null ? recipient.name() : addr.consistentId();

        return connectionManager.channel(recipientConsistentId, address).thenCompose(sender -> sender.send(message))
                .thenCompose(unused -> responseFuture);
    }

    /**
     * Sends a message to the current node.
     *
     * @param msg Message.
     * @param correlationId Correlation id.
     */
    private void sendToSelf(NetworkMessage msg, @Nullable Long correlationId) {
        var address = new NetworkAddress(localAddress.getHostName(), localAddress.getPort(), connectionManager.consistentId());

        for (NetworkMessageHandler networkMessageHandler : getMessageHandlers(msg.groupType())) {
            networkMessageHandler.onReceived(msg, address, correlationId);
        }
    }

    /**
     * Handles an incoming messages.
     *
     * @param consistentId Sender's consistent id.
     * @param msg Incoming message.
     */
    private void onMessage(String consistentId, NetworkMessage msg) {
        if (msg instanceof ScaleCubeMessage) {
            // ScaleCube messages are handled in the ScaleCubeTransport
            return;
        }

        if (msg instanceof InvokeResponse) {
            InvokeResponse response = (InvokeResponse) msg;
            onInvokeResponse(response.message(), response.correlationId());
            return;
        }

        Long correlationId = null;
        NetworkMessage message = msg;

        if (msg instanceof InvokeRequest) {
            // Unwrap invocation request
            InvokeRequest messageWithCorrelation = (InvokeRequest) msg;
            correlationId = messageWithCorrelation.correlationId();
            message = messageWithCorrelation.message();
        }

        ClusterNode sender = topologyService.getByConsistentId(consistentId);

        NetworkAddress senderAddress;

        if (sender != null) {
            senderAddress = sender.address();
        } else {
            // TODO: IGNITE-16373 Use fake address if sender is not in cluster yet. For the response, consistentId from this address will
            //  be used
            senderAddress = new NetworkAddress(UNKNOWN_HOST, UNKNOWN_HOST_PORT, consistentId);
        }

        for (NetworkMessageHandler networkMessageHandler : getMessageHandlers(message.groupType())) {
            // TODO: IGNITE-16373 We should pass ClusterNode and not the address
            networkMessageHandler.onReceived(message, senderAddress, correlationId);
        }
    }

    /**
     * Handles a response to an invocation request.
     *
     * @param response Response message.
     * @param correlationId Request's correlation id.
     */
    private void onInvokeResponse(NetworkMessage response, Long correlationId) {
        CompletableFuture<NetworkMessage> responseFuture = requestsMap.remove(correlationId);
        if (responseFuture != null) {
            responseFuture.complete(response);
        }
    }

    /**
     * Creates an {@link InvokeRequest} from a message and a correlation id.
     *
     * @param message Message.
     * @param correlationId Correlation id.
     * @return Invoke request message.
     */
    private InvokeRequest requestFromMessage(NetworkMessage message, long correlationId) {
        return factory.invokeRequest().correlationId(correlationId).message(message).build();
    }

    /**
     * Creates an {@link InvokeResponse} from a message and a correlation id.
     *
     * @param message Message.
     * @param correlationId Correlation id.
     * @return Invoke response message.
     */
    private InvokeResponse responseFromMessage(NetworkMessage message, long correlationId) {
        return factory.invokeResponse().correlationId(correlationId).message(message).build();
    }

    /**
     * Creates a correlation id for an invocation request.
     *
     * @return New correlation id.
     */
    private long createCorrelationId() {
        return correlationIdGenerator.getAndIncrement();
    }

    /**
     * Checks if the target is the current node.
     *
     * @param target Target cluster node. TODO: IGNITE-16373 May be {@code null} due to the ticket.
     * @param targetSocketAddress Target's socket address.
     * @return {@code true} if the target is the current node, {@code false} otherwise.
     */
    private boolean isSelf(@Nullable ClusterNode target, @Nullable String consistentId, SocketAddress targetSocketAddress) {
        String cid = consistentId;

        if (cid == null && target != null) {
            cid = target.name();
        }

        if (cid != null) {
            return connectionManager.consistentId().equals(cid);
        }

        if (Objects.equals(localAddress, targetSocketAddress)) {
            return true;
        }

        InetSocketAddress targetInetSocketAddress = (InetSocketAddress) targetSocketAddress;

        assert !targetInetSocketAddress.getHostName().equals(UNKNOWN_HOST) && targetInetSocketAddress.getPort() != UNKNOWN_HOST_PORT;

        InetAddress targetInetAddress = targetInetSocketAddress.getAddress();
        if (targetInetAddress.isAnyLocalAddress() || targetInetAddress.isLoopbackAddress()) {
            return targetInetSocketAddress.getPort() == localAddress.getPort();
        }
        return false;
    }

    /**
     * Stops the messaging service.
     */
    public void stop() {
        var exception = new NodeStoppingException();

        requestsMap.values().forEach(fut -> fut.completeExceptionally(exception));

        requestsMap.clear();
    }
}
