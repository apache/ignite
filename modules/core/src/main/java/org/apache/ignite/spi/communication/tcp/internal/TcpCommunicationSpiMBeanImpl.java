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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiMBeanAdapter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;

/**
 * MBean implementation for TcpCommunicationSpi.
 */
public class TcpCommunicationSpiMBeanImpl extends IgniteSpiMBeanAdapter implements TcpCommunicationSpiMBean {
    /** Statistics. */
    private final TcpCommunicationMetricsListener metricsLsnr;

    /** Config. */
    private final TcpCommunicationConfiguration cfg;

    /** State provider. */
    private final ClusterStateProvider stateProvider;
    
    /** {@inheritDoc} */
    public TcpCommunicationSpiMBeanImpl(
        IgniteSpiAdapter spiAdapter,
        TcpCommunicationMetricsListener metricsLsnr,
        TcpCommunicationConfiguration cfg,
        ClusterStateProvider stateProvider
    ) {
        super(spiAdapter);
        this.metricsLsnr = metricsLsnr;
        this.cfg = cfg;
        this.stateProvider = stateProvider;
    }

    /** {@inheritDoc} */
    @Override public String getLocalAddress() {
        return cfg.localAddress();
    }

    /** {@inheritDoc} */
    @Override public int getLocalPort() {
        return cfg.localPort();
    }

    /** {@inheritDoc} */
    @Override public int getLocalPortRange() {
        return cfg.localPortRange();
    }

    /** {@inheritDoc} */
    @Override public boolean isUsePairedConnections() {
        return cfg.usePairedConnections();
    }

    /** {@inheritDoc} */
    @Override public int getConnectionsPerNode() {
        return cfg.connectionsPerNode();
    }

    /** {@inheritDoc} */
    @Override public int getSharedMemoryPort() {
        return cfg.shmemPort();
    }

    /** {@inheritDoc} */
    @Override public long getIdleConnectionTimeout() {
        return cfg.idleConnectionTimeout();
    }

    /** {@inheritDoc} */
    @Override public long getSocketWriteTimeout() {
        return cfg.socketWriteTimeout();
    }

    /** {@inheritDoc} */
    @Override public int getAckSendThreshold() {
        return cfg.ackSendThreshold();
    }

    /** {@inheritDoc} */
    @Override public int getUnacknowledgedMessagesBufferSize() {
        return cfg.unackedMsgsBufferSize();
    }

    /** {@inheritDoc} */
    @Override public long getConnectTimeout() {
        return cfg.connectionTimeout();
    }

    /** {@inheritDoc} */
    @Override public long getMaxConnectTimeout() {
        return cfg.maxConnectionTimeout();
    }

    /** {@inheritDoc} */
    @Override public int getReconnectCount() {
        return cfg.reconCount();
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectBuffer() {
        return cfg.directBuffer();
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectSendBuffer() {
        return cfg.directSendBuffer();
    }

    /** {@inheritDoc} */
    @Override public int getSelectorsCount() {
        return cfg.selectorsCount();
    }

    /** {@inheritDoc} */
    @Override public long getSelectorSpins() {
        return cfg.selectorSpins();
    }

    /** {@inheritDoc} */
    @Override public boolean isTcpNoDelay() {
        return cfg.tcpNoDelay();
    }

    /** {@inheritDoc} */
    @Override public int getSocketReceiveBuffer() {
        return cfg.socketReceiveBuffer();
    }

    /** {@inheritDoc} */
    @Override public int getSocketSendBuffer() {
        return cfg.socketSendBuffer();
    }

    /** {@inheritDoc} */
    @Override public int getMessageQueueLimit() {
        return cfg.messageQueueLimit();
    }

    /** {@inheritDoc} */
    @Override public int getSlowClientQueueLimit() {
        return cfg.slowClientQueueLimit();
    }

    /** {@inheritDoc} */
    @Override public void dumpStats() {
        stateProvider.dumpStats();
    }

    /** {@inheritDoc} */
    @Override public int getSentMessagesCount() {
        return metricsLsnr.sentMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getSentBytesCount() {
        return metricsLsnr.sentBytesCount();
    }

    /** {@inheritDoc} */
    @Override public int getReceivedMessagesCount() {
        return metricsLsnr.receivedMessagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getReceivedBytesCount() {
        return metricsLsnr.receivedBytesCount();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getReceivedMessagesByType() {
        return metricsLsnr.receivedMessagesByType();
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Long> getReceivedMessagesByNode() {
        return metricsLsnr.receivedMessagesByNode();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> getSentMessagesByType() {
        return metricsLsnr.sentMessagesByType();
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Long> getSentMessagesByNode() {
        return metricsLsnr.sentMessagesByNode();
    }

    /** {@inheritDoc} */
    @Override public int getOutboundMessagesQueueSize() {
        return stateProvider.getOutboundMessagesQueueSize();
    }
}
