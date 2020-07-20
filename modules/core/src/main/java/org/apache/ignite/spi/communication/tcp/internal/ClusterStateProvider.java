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

package org.apache.ignite.spi.communication.tcp.internal;

import java.util.UUID;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.discovery.DiscoverySpi;

import static org.apache.ignite.internal.IgniteFeatures.TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE;

/**
 * The role of this is aggregate logic of cluster states.
 *
 * @deprecated This class must be removed.
 */
public class ClusterStateProvider {
    /** Ignite. */
    private final Ignite ignite;

    /** Local node supplier. */
    private final Supplier<ClusterNode> locNodeSupplier;

    /** Tcp communication spi. */
    private final TcpCommunicationSpi tcpCommSpi;

    /** Stopped supplier. */
    private final Supplier<Boolean> stoppedSupplier;

    /** Spi context without latch supplier. */
    private final Supplier<IgniteSpiContext> spiCtxWithoutLatchSupplier;

    /** Logger. */
    private final IgniteLogger log;

    /** Ignite ex supplier. */
    private final Supplier<Ignite> igniteExSupplier;

    /**
     * @param ignite Ignite.
     * @param locNodeSupplier Local node supplier.
     * @param tcpCommSpi Tcp communication spi.
     * @param stoppedSupplier Stopped supplier.
     * @param spiCtxWithoutLatchSupplier Spi context without latch
     * @param log Logger.
     * @param igniteExSupplier Returns already exists instance from spi.
     */
    public ClusterStateProvider(
        Ignite ignite,
        Supplier<ClusterNode> locNodeSupplier,
        TcpCommunicationSpi tcpCommSpi,
        Supplier<Boolean> stoppedSupplier,
        Supplier<IgniteSpiContext> spiCtxWithoutLatchSupplier,
        IgniteLogger log,
        Supplier<Ignite> igniteExSupplier
    ) {
        this.ignite = ignite;
        this.locNodeSupplier = locNodeSupplier;
        this.tcpCommSpi = tcpCommSpi;
        this.stoppedSupplier = stoppedSupplier;
        this.spiCtxWithoutLatchSupplier = spiCtxWithoutLatchSupplier;
        this.log = log;
        this.igniteExSupplier = igniteExSupplier;
    }

    /**
     * @return {@code True} if local node in disconnected state.
     */
    public boolean isLocalNodeDisconnected() {
        boolean disconnected = false;

        if (ignite instanceof IgniteKernal)
            disconnected = ((IgniteEx)ignite).context().clientDisconnected();

        return disconnected;
    }

    /**
     * @return {@code True} if ssl enabled.
     */
    public boolean isSslEnabled() {
        return ignite.configuration().getSslContextFactory() != null;
    }

    /**
     * @return {@link SSLEngine} for ssl connections.
     */
    public SSLEngine createSSLEngine() {
        return ignite.configuration().getSslContextFactory().create().createSSLEngine();
    }

    /**
     * @return {@code true} if {@link TcpCommunicationSpi} stopped.
     */
    public boolean isStopping() {
        return stoppedSupplier.get();
    }

    /**
     * Returns client failure detection timeout set to use for network related operations.
     *
     * @return client failure detection timeout in milliseconds or {@code 0} if the timeout is disabled.
     */
    public long clientFailureDetectionTimeout() {
        return tcpCommSpi.clientFailureDetectionTimeout();
    }

    /**
     * @return {@link IgniteSpiContext} of {@link TcpCommunicationSpi}.
     */
    public IgniteSpiContext getSpiContext() {
        return tcpCommSpi.getSpiContext();
    }

    /**
     * @return {@link IgniteSpiContext} of {@link TcpCommunicationSpi}.
     */
    public IgniteSpiContext getSpiContextWithoutInitialLatch() {
        return spiCtxWithoutLatchSupplier.get();
    }

    /**
     * @return Outbound messages queue size.
     */
    public int getOutboundMessagesQueueSize() {
        return tcpCommSpi.getOutboundMessagesQueueSize();
    }

    /**
     * Makes dump of {@link TcpCommunicationSpi} stats.
     */
    public void dumpStats() {
        tcpCommSpi.dumpStats();
    }

    /**
     * Checks whether remote nodes support {@link HandshakeWaitMessage}.
     *
     * @return {@code True} if remote nodes support {@link HandshakeWaitMessage}.
     */
    public boolean isHandshakeWaitSupported() {
        GridKernalContext ctx = (ignite instanceof IgniteEx) ? ((IgniteEx)ignite).context() : null;

        DiscoverySpi discoSpi = ignite.configuration().getDiscoverySpi();

        return IgniteFeatures.allNodesSupport(ctx, discoSpi, TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE);
    }

    /**
     * @return Node ID message.
     */
    public NodeIdMessage nodeIdMessage() {
        final UUID locNodeId = (ignite != null && ignite instanceof IgniteKernal) ? ((IgniteEx)ignite).context().localNodeId() :
            safeLocalNodeId();

        return new NodeIdMessage(locNodeId);
    }

    /**
     * @return Local node ID.
     */
    private UUID safeLocalNodeId() {
        ClusterNode locNode = locNodeSupplier.get();

        UUID id;

        if (locNode == null) {
            U.warn(log, "Local node is not started or fully initialized [isStopping=" +
                isStopping() + ']');

            id = new UUID(0, 0);
        }
        else
            id = locNode.id();

        return id;
    }
}
